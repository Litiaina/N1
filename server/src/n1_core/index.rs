use crate::enums::app_error_enum::AppError;
use crate::n1_core::constants::{PREFIX_DELIMITER_SEGMENT_PREFIX, PREFIX_SEGMENT_EXT};
use crate::n1_core::disk_manager::{
    ObjectMetadata, ObjectPathManager, PartMetadata, PrefixIndexEntry, PrefixIndexSegment,
    ReservationReceipt, compare_version_recency, version_recency_is_newer,
};
use crate::n1_core::prefix_delimiter::build_delimiter_compacted_from_entries;
use crate::n1_core::upload::{PartCommitReceipt, UploadSessionManifest};
use chrono::Utc;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
#[cfg(all(unix, not(test)))]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::SystemTime;
use tokio::fs;
use tokio::io::AsyncWriteExt;
#[cfg(not(test))]
use tokio::sync::Semaphore;
use tokio::sync::{Mutex, mpsc, oneshot};
#[cfg(not(test))]
use tokio::time::sleep;
use tokio::time::{Duration, Instant, timeout_at};
use tracing::warn;
use ulid::Ulid;

const INDEX_DIR: &str = "index";
const INDEX_PARTITIONS_DIR: &str = "partitions";
const INDEX_SESSIONS_DIR: &str = "sessions";
const INDEX_OBJECTS_DIR: &str = "objects";
const INDEX_TRASH_DIR: &str = "trash";
const INDEX_WAL_DIR: &str = "wal";
const INDEX_WAL_SEGMENTS_DIR: &str = "segments";
const INDEX_WAL_MANIFEST_FILE: &str = "manifest.msgpack";
const INDEX_CHECKPOINTS_DIR: &str = "checkpoints";
const INDEX_LATEST_CHECKPOINT_FILE: &str = "latest.msgpack";
const INDEX_PREFIX_PROJECTION_CURSOR_FILE: &str = "prefix_projection.cursor.msgpack";
const INDEX_LATEST_SUFFIX: &str = ".latest.msgpack";
const INDEX_SESSION_SUFFIX: &str = ".session.msgpack";
const INDEX_SESSION_REMOVED_SUFFIX: &str = ".removed.msgpack";
const INDEX_OBJECT_SUFFIX: &str = ".msgpack";
const INDEX_PARTITION_COUNT: u16 = 256;
const INDEX_WAL_SEGMENT_MAX_BYTES: u64 = 4 * 1024 * 1024;
const INDEX_CHECKPOINT_RECORD_INTERVAL: u64 = 64;
const INDEX_WAL_MANIFEST_PERSIST_INTERVAL: u64 = 64;
const INDEX_WAL_GROUP_COMMIT_WINDOW_MS: u64 = 4;
const INDEX_WAL_GROUP_COMMIT_MAX_RECORDS: usize = 32;
const INDEX_FRAGMENT_VERSION_STREAM_BUFFER: usize = 64;

static INDEX_WAL_APPENDERS: LazyLock<Mutex<HashMap<String, mpsc::Sender<WalAppendRequest>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static INDEX_WAL_MANIFEST_CACHE: LazyLock<Mutex<HashMap<String, IndexWalManifest>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static INDEX_ACTIVE_SESSIONS: LazyLock<Mutex<HashMap<String, HashMap<String, IndexSessionState>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static INDEX_ACTIVE_SESSION_FRAGMENTS_SCANNED: LazyLock<Mutex<HashSet<String>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));
static INDEX_PARTITION_RUNTIME: LazyLock<
    Mutex<HashMap<String, Arc<Mutex<PartitionRuntimeState>>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
static INDEX_SESSION_LOCKS: LazyLock<Mutex<HashMap<String, Arc<Mutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSessionState {
    pub manifest: UploadSessionManifest,
    #[serde(default)]
    pub reservation: Option<ReservationReceipt>,
    #[serde(default)]
    pub parts: BTreeMap<u64, PartMetadata>,
    #[serde(default)]
    pub last_activity_at: i64,
    #[serde(default)]
    pub finalized_at: Option<i64>,
    #[serde(default)]
    pub aborted_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemovedSessionReceipt {
    pub state: IndexSessionState,
    pub removed_at: i64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexLatestState {
    pub version_id: String,
    pub metadata: ObjectMetadata,
}

#[derive(Debug, Clone, Default)]
struct PartitionMaterializedState {
    live_latest: HashMap<String, IndexLatestState>,
    trash_latest: HashMap<String, IndexLatestState>,
    sessions: HashMap<String, IndexSessionState>,
}

#[derive(Debug, Clone)]
struct PartitionRuntimeState {
    manifest: IndexWalManifest,
    materialized: PartitionMaterializedState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum IndexWalRecord {
    SessionOpened {
        session: IndexSessionState,
    },
    PartCommitted {
        fragment: String,
        version_id: String,
        part_index: u64,
        part: SessionPartCommitRecord,
        committed_at: i64,
    },
    VersionPublished {
        metadata: ObjectMetadata,
        published_at: i64,
    },
    VersionTrashed {
        metadata: ObjectMetadata,
        trashed_at: i64,
    },
    VersionRemoved {
        fragment: String,
        object_key: String,
        version_id: String,
        removed_at: i64,
        from_trash: bool,
        #[serde(default)]
        next_latest: Option<IndexLatestState>,
    },
    SessionRemoved {
        fragment: String,
        version_id: String,
        removed_at: i64,
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum SessionPartCommitRecord {
    Full(PartMetadata),
    PlannedReceipt(PartCommitReceipt),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexWalEnvelope {
    sequence: u64,
    recorded_at: i64,
    record: IndexWalRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexWalManifest {
    active_segment_id: u64,
    next_sequence: u64,
    last_checkpoint_sequence: u64,
}

#[derive(Debug)]
struct WalAppendRequest {
    record: IndexWalRecord,
    response: oneshot::Sender<Result<u64, AppError>>,
}

impl Default for IndexWalManifest {
    fn default() -> Self {
        Self {
            active_segment_id: 1,
            next_sequence: 1,
            last_checkpoint_sequence: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexCheckpoint {
    created_at: i64,
    up_to_sequence: u64,
    #[serde(default)]
    live_latest: Vec<IndexLatestState>,
    #[serde(default)]
    trash_latest: Vec<IndexLatestState>,
    #[serde(default)]
    sessions: Vec<IndexSessionState>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PrefixProjectionCursor {
    #[serde(default)]
    sources: HashMap<String, u64>,
}

#[derive(Debug, Clone, Copy, Default)]
struct PrefixProjectionTaskState {
    running: bool,
    rerun_requested: bool,
    #[cfg(not(test))]
    recovery_scan_needed: bool,
}

#[derive(Debug, Clone, Default)]
struct PrefixProjectionPendingState {
    task: PrefixProjectionTaskState,
    #[cfg(not(test))]
    dirty_keys: HashSet<String>,
}

static PREFIX_PROJECTION_TASKS: LazyLock<Mutex<HashMap<String, PrefixProjectionPendingState>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(not(test))]
#[derive(Debug, Clone, Copy, Default)]
struct PartitionMaintenanceTaskState {
    running: bool,
    rerun_requested: bool,
}
#[cfg(not(test))]
static INDEX_PARTITION_MAINTENANCE_TASKS: LazyLock<
    Mutex<HashMap<String, PartitionMaintenanceTaskState>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(not(test))]
static PREFIX_PROJECTION_IO_GATE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(1));
#[cfg(not(test))]
static INDEX_REPLICA_IO_GATE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(4));
#[cfg(not(test))]
static INDEX_BACKGROUND_IO_GATE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(2));

#[derive(Debug, Clone)]
struct IndexRoute {
    partition_id: u16,
    roots: Vec<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct UploadRuntimeStats {
    pub active_fragments: u64,
    pub active_sessions: u64,
    pub reserved_bytes: u64,
    pub committed_parts: u64,
    pub committed_bytes: u64,
    pub oldest_session_age_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct IndexRuntimeStats {
    pub loaded_partition_runtimes: u64,
    pub wal_appenders: u64,
    pub prefix_projection_fragments_pending: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexReplicaWriteMode {
    BackgroundBestEffort,
    SyncBestEffort,
}

fn normalize_session_state(mut state: IndexSessionState) -> IndexSessionState {
    if state.last_activity_at == 0 {
        state.last_activity_at = state.manifest.created_at;
    }
    state
}

fn runtime_key(root: &Path, fragment: &str, partition_id: u16) -> String {
    format!("{}:{}:{:04x}", root.display(), fragment, partition_id)
}

fn checkpoint_to_materialized(checkpoint: IndexCheckpoint) -> PartitionMaterializedState {
    PartitionMaterializedState {
        live_latest: checkpoint
            .live_latest
            .into_iter()
            .map(|state| (canonical_key(&state.metadata.object_key), state))
            .collect(),
        trash_latest: checkpoint
            .trash_latest
            .into_iter()
            .map(|state| (canonical_key(&state.metadata.object_key), state))
            .collect(),
        sessions: checkpoint
            .sessions
            .into_iter()
            .map(normalize_session_state)
            .map(|state| (state.manifest.version_id.clone(), state))
            .collect(),
    }
}

fn materialized_to_checkpoint(
    materialized: &PartitionMaterializedState,
    up_to_sequence: u64,
) -> IndexCheckpoint {
    IndexCheckpoint {
        created_at: Utc::now().timestamp(),
        up_to_sequence,
        live_latest: materialized.live_latest.values().cloned().collect(),
        trash_latest: materialized.trash_latest.values().cloned().collect(),
        sessions: materialized.sessions.values().cloned().collect(),
    }
}

fn apply_wal_record_to_materialized(
    materialized: &mut PartitionMaterializedState,
    record: &IndexWalRecord,
) {
    match record {
        IndexWalRecord::SessionOpened { session } => {
            materialized.sessions.insert(
                session.manifest.version_id.clone(),
                normalize_session_state(session.clone()),
            );
        }
        IndexWalRecord::PartCommitted {
            fragment,
            version_id,
            part_index,
            part,
            committed_at,
        } => {
            if let Some(state) = materialized.sessions.get_mut(version_id) {
                if state.manifest.fragment == *fragment {
                    if let Some(materialized_part) =
                        materialize_session_part_record(&state.manifest, *part_index, part)
                    {
                        state.parts.insert(*part_index, materialized_part);
                        state.last_activity_at = *committed_at;
                    }
                }
            }
        }
        IndexWalRecord::SessionRemoved { version_id, .. } => {
            materialized.sessions.remove(version_id);
        }
        IndexWalRecord::VersionPublished { metadata, .. } => {
            let key = canonical_key(&metadata.object_key);
            let candidate = IndexLatestState {
                version_id: metadata.version_id.clone(),
                metadata: metadata.clone(),
            };
            match materialized.live_latest.get(&key) {
                Some(current) if !metadata_is_newer(&candidate.metadata, &current.metadata) => {}
                _ => {
                    materialized.live_latest.insert(key, candidate);
                }
            }
        }
        IndexWalRecord::VersionTrashed { metadata, .. } => {
            let key = canonical_key(&metadata.object_key);
            let candidate = IndexLatestState {
                version_id: metadata.version_id.clone(),
                metadata: metadata.clone(),
            };
            match materialized.trash_latest.get(&key) {
                Some(current) if !metadata_is_newer(&candidate.metadata, &current.metadata) => {}
                _ => {
                    materialized.trash_latest.insert(key, candidate);
                }
            }
        }
        IndexWalRecord::VersionRemoved {
            object_key,
            from_trash,
            next_latest,
            ..
        } => {
            let key = canonical_key(object_key);
            let target = if *from_trash {
                &mut materialized.trash_latest
            } else {
                &mut materialized.live_latest
            };
            match next_latest {
                Some(state) => {
                    target.insert(key, state.clone());
                }
                None => {
                    target.remove(&key);
                }
            }
        }
    }
}

fn hash_key(parts: &[&str]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    hasher.finalize().into()
}

fn object_partition_route(fragment: &str, object_key: &str) -> Result<IndexRoute, AppError> {
    route_for_hash(hash_key(&["object", fragment, object_key]))
}

pub(crate) fn object_partition_id(fragment: &str, object_key: &str) -> Result<u16, AppError> {
    Ok(object_partition_route(fragment, object_key)?.partition_id)
}

fn session_partition_route(fragment: &str, version_id: &str) -> Result<IndexRoute, AppError> {
    route_for_hash(hash_key(&["session", fragment, version_id]))
}

fn session_cache_key(fragment: &str, version_id: &str) -> String {
    format!("{}:{}", fragment, version_id)
}

async fn cache_active_session(state: &IndexSessionState) {
    if state.finalized_at.is_some() || state.aborted_at.is_some() {
        return;
    }
    let mut guard = INDEX_ACTIVE_SESSIONS.lock().await;
    guard
        .entry(state.manifest.fragment.clone())
        .or_default()
        .insert(state.manifest.version_id.clone(), state.clone());
    INDEX_ACTIVE_SESSION_FRAGMENTS_SCANNED
        .lock()
        .await
        .insert(state.manifest.fragment.clone());
}

async fn remove_active_session(fragment: &str, version_id: &str) {
    let mut guard = INDEX_ACTIVE_SESSIONS.lock().await;
    if let Some(fragment_sessions) = guard.get_mut(fragment) {
        fragment_sessions.remove(version_id);
    }
    INDEX_ACTIVE_SESSION_FRAGMENTS_SCANNED
        .lock()
        .await
        .insert(fragment.to_string());
}

fn materialize_session_part_record(
    manifest: &UploadSessionManifest,
    part_index: u64,
    record: &SessionPartCommitRecord,
) -> Option<PartMetadata> {
    match record {
        SessionPartCommitRecord::Full(part) => Some(part.clone()),
        SessionPartCommitRecord::PlannedReceipt(receipt) => {
            manifest.materialize_part_from_receipt(part_index, receipt)
        }
    }
}

async fn session_lock(fragment: &str, version_id: &str) -> Arc<Mutex<()>> {
    let key = session_cache_key(fragment, version_id);
    let mut guard = INDEX_SESSION_LOCKS.lock().await;
    guard
        .entry(key)
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

fn route_for_hash(hash: [u8; 32]) -> Result<IndexRoute, AppError> {
    let partition_id = (u16::from(hash[0]) << 8 | u16::from(hash[1])) % INDEX_PARTITION_COUNT;
    route_for_partition_seed(partition_id, hash[2], hash[3], hash[4])
}

fn route_for_partition_id(partition_id: u16) -> Result<IndexRoute, AppError> {
    route_for_partition_seed(partition_id, 0, 0, 0)
}

fn route_for_partition_seed(
    partition_id: u16,
    owner_selector: u8,
    replica_selector: u8,
    replica_root_selector: u8,
) -> Result<IndexRoute, AppError> {
    let roots = ObjectPathManager::new().get_all_index_roots();
    if roots.is_empty() {
        return Err(AppError::InternalServerError(
            "No index roots configured".to_string(),
        ));
    }

    let mut groups: Vec<Vec<PathBuf>> = Vec::new();
    let mut group_index = HashMap::<String, usize>::new();
    for root in roots {
        #[cfg(test)]
        let key = root.to_string_lossy().to_string();
        #[cfg(all(unix, not(test)))]
        let key = std::fs::metadata(&root)
            .ok()
            .map(|meta| format!("dev:{}", meta.dev()))
            .unwrap_or_else(|| root.to_string_lossy().to_string());
        #[cfg(all(not(unix), not(test)))]
        let key = root.to_string_lossy().to_string();
        if let Some(idx) = group_index.get(&key).copied() {
            groups[idx].push(root);
        } else {
            group_index.insert(key, groups.len());
            groups.push(vec![root]);
        }
    }

    let owner_group_idx = usize::from(partition_id) % groups.len();
    let owner_group = &groups[owner_group_idx];
    let owner_root = owner_group[usize::from(owner_selector) % owner_group.len()].clone();
    let mut selected = vec![owner_root];
    if groups.len() > 1 {
        let replica_offset = 1
            + ((usize::from(partition_id >> 8) + usize::from(replica_selector))
                % (groups.len() - 1));
        let replica_group_idx = (owner_group_idx + replica_offset) % groups.len();
        let replica_group = &groups[replica_group_idx];
        let replica_root =
            replica_group[usize::from(replica_root_selector) % replica_group.len()].clone();
        if replica_root != selected[0] {
            selected.push(replica_root);
        }
    }

    Ok(IndexRoute {
        partition_id,
        roots: selected,
    })
}

fn partition_dir(root: &Path, fragment: &str, partition_id: u16) -> PathBuf {
    root.join(fragment)
        .join(INDEX_DIR)
        .join(INDEX_PARTITIONS_DIR)
        .join(format!("{:04x}", partition_id))
}

fn session_state_path(root: &Path, fragment: &str, partition_id: u16, version_id: &str) -> PathBuf {
    partition_dir(root, fragment, partition_id)
        .join(INDEX_SESSIONS_DIR)
        .join(format!("{}{}", version_id, INDEX_SESSION_SUFFIX))
}

fn session_parts_dir(root: &Path, fragment: &str, partition_id: u16, version_id: &str) -> PathBuf {
    partition_dir(root, fragment, partition_id)
        .join(INDEX_SESSIONS_DIR)
        .join(format!("{}.parts", version_id))
}

fn session_removed_receipt_path(
    root: &Path,
    fragment: &str,
    partition_id: u16,
    version_id: &str,
) -> PathBuf {
    partition_dir(root, fragment, partition_id)
        .join(INDEX_SESSIONS_DIR)
        .join(format!("{}{}", version_id, INDEX_SESSION_REMOVED_SUFFIX))
}

fn object_prefix_dir_for_state(
    root: &Path,
    fragment: &str,
    partition_id: u16,
    object_key: &str,
    trashed: bool,
) -> PathBuf {
    let key_hash = ObjectPathManager::object_key_hash(object_key);
    partition_dir(root, fragment, partition_id)
        .join(if trashed {
            INDEX_TRASH_DIR
        } else {
            INDEX_OBJECTS_DIR
        })
        .join(&key_hash[0..2])
        .join(key_hash)
}

fn latest_state_path_for_state(
    root: &Path,
    fragment: &str,
    partition_id: u16,
    object_key: &str,
    trashed: bool,
) -> PathBuf {
    object_prefix_dir_for_state(root, fragment, partition_id, object_key, trashed)
        .join(format!("latest{}", INDEX_LATEST_SUFFIX))
}

fn object_version_path_for_state(
    root: &Path,
    fragment: &str,
    partition_id: u16,
    object_key: &str,
    version_id: &str,
    trashed: bool,
) -> PathBuf {
    object_prefix_dir_for_state(root, fragment, partition_id, object_key, trashed)
        .join(format!("{}{}", version_id, INDEX_OBJECT_SUFFIX))
}

fn wal_dir(root: &Path, fragment: &str, partition_id: u16) -> PathBuf {
    partition_dir(root, fragment, partition_id).join(INDEX_WAL_DIR)
}

fn wal_segments_dir(root: &Path, fragment: &str, partition_id: u16) -> PathBuf {
    wal_dir(root, fragment, partition_id).join(INDEX_WAL_SEGMENTS_DIR)
}

fn wal_manifest_path(root: &Path, fragment: &str, partition_id: u16) -> PathBuf {
    wal_dir(root, fragment, partition_id).join(INDEX_WAL_MANIFEST_FILE)
}

fn wal_segment_path(root: &Path, fragment: &str, partition_id: u16, segment_id: u64) -> PathBuf {
    wal_segments_dir(root, fragment, partition_id).join(format!("{:020}.wal", segment_id))
}

fn checkpoints_dir(root: &Path, fragment: &str, partition_id: u16) -> PathBuf {
    partition_dir(root, fragment, partition_id).join(INDEX_CHECKPOINTS_DIR)
}

fn latest_checkpoint_path(root: &Path, fragment: &str, partition_id: u16) -> PathBuf {
    checkpoints_dir(root, fragment, partition_id).join(INDEX_LATEST_CHECKPOINT_FILE)
}

fn prefix_projection_cursor_path(root: &Path, fragment: &str) -> PathBuf {
    root.join(fragment)
        .join(INDEX_DIR)
        .join(INDEX_PREFIX_PROJECTION_CURSOR_FILE)
}

#[cfg(unix)]
async fn sync_dir(path: &Path) -> Result<(), AppError> {
    let dir = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY)
        .open(path)
        .await
        .map_err(|e| AppError::Io(e, format!("open dir for sync: {}", path.display())))?;
    dir.sync_all()
        .await
        .map_err(|e| AppError::Io(e, format!("sync dir: {}", path.display())))?;
    Ok(())
}

#[cfg(windows)]
async fn sync_dir(path: &Path) -> Result<(), AppError> {
    use std::os::windows::fs::OpenOptionsExt;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x02000000;

    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
        .await
        .map_err(|e| AppError::Io(e, format!("open dir for sync: {}", path.display())))?;
    file.sync_all()
        .await
        .map_err(|e| AppError::Io(e, format!("sync dir: {}", path.display())))?;
    Ok(())
}

#[cfg(not(any(unix, windows)))]
async fn sync_dir(_path: &Path) -> Result<(), AppError> {
    Ok(())
}

async fn write_atomic(path: &Path, data: &[u8]) -> Result<(), AppError> {
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError(format!("No parent dir for {}", path.display()))
    })?;
    fs::create_dir_all(parent)
        .await
        .map_err(|e| AppError::Io(e, format!("mkdir {}", parent.display())))?;

    let tmp = parent.join(format!(
        ".{}.tmp.{}",
        path.file_name().and_then(|v| v.to_str()).unwrap_or("state"),
        Ulid::new()
    ));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .await
        .map_err(|e| AppError::Io(e, format!("open {}", tmp.display())))?;
    file.write_all(data)
        .await
        .map_err(|e| AppError::Io(e, format!("write {}", tmp.display())))?;
    file.sync_all()
        .await
        .map_err(|e| AppError::Io(e, format!("sync {}", tmp.display())))?;
    drop(file);

    fs::rename(&tmp, path)
        .await
        .map_err(|e| AppError::Io(e, format!("rename {} -> {}", tmp.display(), path.display())))?;
    sync_dir(parent).await?;
    Ok(())
}

async fn read_messagepack<T>(path: &Path) -> Result<T, AppError>
where
    T: for<'de> Deserialize<'de>,
{
    let bytes = fs::read(path)
        .await
        .map_err(|e| AppError::Io(e, format!("read {}", path.display())))?;
    rmp_serde::from_slice(&bytes)
        .map_err(|e| AppError::InternalServerError(format!("decode {}: {}", path.display(), e)))
}

async fn read_messagepack_if_exists<T>(path: &Path) -> Result<Option<T>, AppError>
where
    T: for<'de> Deserialize<'de>,
{
    if !fs::try_exists(path).await.unwrap_or(false) {
        return Ok(None);
    }
    read_messagepack(path).await.map(Some)
}

async fn open_wal_segment_for_append(path: &Path) -> Result<(fs::File, u64, bool), AppError> {
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError(format!("No parent dir for {}", path.display()))
    })?;
    fs::create_dir_all(parent)
        .await
        .map_err(|e| AppError::Io(e, format!("mkdir {}", parent.display())))?;
    let existed = fs::try_exists(path).await.unwrap_or(false);
    let len = if existed {
        fs::metadata(path)
            .await
            .map_err(|e| AppError::Io(e, format!("metadata {}", path.display())))?
            .len()
    } else {
        0
    };
    let file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .map_err(|e| AppError::Io(e, format!("open {}", path.display())))?;
    Ok((file, len, !existed))
}

async fn flush_wal_segment(
    file: &mut fs::File,
    path: &Path,
    created_new: bool,
) -> Result<(), AppError> {
    file.sync_data()
        .await
        .map_err(|e| AppError::Io(e, format!("sync wal {}", path.display())))?;
    if created_new {
        if let Some(parent) = path.parent() {
            sync_dir(parent).await?;
        }
    }
    Ok(())
}

async fn get_wal_appender(
    root: &Path,
    fragment: &str,
    partition_id: u16,
) -> mpsc::Sender<WalAppendRequest> {
    let key = format!("{}:{}:{:04x}", root.display(), fragment, partition_id);
    let mut guard = INDEX_WAL_APPENDERS.lock().await;
    if let Some(existing) = guard.get(&key) {
        if !existing.is_closed() {
            return existing.clone();
        }
    }

    let (tx, rx) = mpsc::channel::<WalAppendRequest>(INDEX_WAL_GROUP_COMMIT_MAX_RECORDS * 8);
    let root_path = root.to_path_buf();
    let fragment_name = fragment.to_string();
    tokio::spawn(async move {
        wal_appender_task(root_path, fragment_name, partition_id, rx).await;
    });
    guard.insert(key, tx.clone());
    tx
}

async fn wal_appender_task(
    root: PathBuf,
    fragment: String,
    partition_id: u16,
    mut rx: mpsc::Receiver<WalAppendRequest>,
) {
    let manifest = match load_wal_manifest(&root, &fragment, partition_id).await {
        Ok(manifest) => manifest,
        Err(e) => {
            while let Some(request) = rx.recv().await {
                let _ = request
                    .response
                    .send(Err(AppError::InternalServerError(format!(
                        "load wal manifest {}:{}:{:04x}: {}",
                        root.display(),
                        fragment,
                        partition_id,
                        e
                    ))));
            }
            return;
        }
    };
    let runtime =
        match load_partition_runtime_state(&root, &fragment, partition_id, &manifest).await {
            Ok(state) => Arc::new(Mutex::new(state)),
            Err(e) => {
                while let Some(request) = rx.recv().await {
                    let _ = request
                        .response
                        .send(Err(AppError::InternalServerError(format!(
                            "load partition runtime {}:{}:{:04x}: {}",
                            root.display(),
                            fragment,
                            partition_id,
                            e
                        ))));
                }
                return;
            }
        };
    INDEX_PARTITION_RUNTIME
        .lock()
        .await
        .insert(runtime_key(&root, &fragment, partition_id), runtime.clone());

    let mut active_segment_id = manifest.active_segment_id;
    let mut active_segment_path =
        wal_segment_path(&root, &fragment, partition_id, active_segment_id);
    let mut active_file: Option<fs::File> = None;
    let mut active_len: u64 = 0;
    let mut active_file_created_new = false;

    while let Some(first) = rx.recv().await {
        let mut batch = vec![first];
        let deadline = Instant::now() + Duration::from_millis(INDEX_WAL_GROUP_COMMIT_WINDOW_MS);
        while batch.len() < INDEX_WAL_GROUP_COMMIT_MAX_RECORDS {
            match timeout_at(deadline, rx.recv()).await {
                Ok(Some(request)) => batch.push(request),
                Ok(None) | Err(_) => break,
            }
        }

        let mut replies: Vec<(oneshot::Sender<Result<u64, AppError>>, u64)> =
            Vec::with_capacity(batch.len());
        let mut wrote_current_segment = false;
        let mut rotated = false;
        let mut batch_failed = None::<String>;
        let mut runtime_guard = runtime.lock().await;

        for request in batch {
            let payload = match rmp_serde::to_vec(&IndexWalEnvelope {
                sequence: runtime_guard.manifest.next_sequence,
                recorded_at: Utc::now().timestamp(),
                record: request.record.clone(),
            }) {
                Ok(payload) => payload,
                Err(e) => {
                    batch_failed = Some(format!("serialize wal record: {}", e));
                    let _ = request.response.send(Err(AppError::InternalServerError(
                        batch_failed.clone().unwrap(),
                    )));
                    break;
                }
            };
            let payload_len = match u32::try_from(payload.len()) {
                Ok(len) => len,
                Err(_) => {
                    batch_failed = Some("WAL record too large".to_string());
                    let _ = request.response.send(Err(AppError::InternalServerError(
                        batch_failed.clone().unwrap(),
                    )));
                    break;
                }
            };
            let mut frame = Vec::with_capacity(4 + payload.len());
            frame.extend_from_slice(&payload_len.to_le_bytes());
            frame.extend_from_slice(&payload);
            let frame_len = u64::try_from(frame.len()).unwrap_or(u64::MAX);

            if active_file.is_some()
                && active_len > 0
                && active_len.saturating_add(frame_len) > INDEX_WAL_SEGMENT_MAX_BYTES
            {
                if wrote_current_segment {
                    if let Some(file) = active_file.as_mut() {
                        if let Err(e) =
                            flush_wal_segment(file, &active_segment_path, active_file_created_new)
                                .await
                        {
                            batch_failed = Some(e.to_string());
                            let _ = request.response.send(Err(AppError::InternalServerError(
                                batch_failed.clone().unwrap(),
                            )));
                            break;
                        }
                    }
                    wrote_current_segment = false;
                }
                active_segment_id += 1;
                runtime_guard.manifest.active_segment_id = active_segment_id;
                active_segment_path =
                    wal_segment_path(&root, &fragment, partition_id, active_segment_id);
                active_file = None;
                active_len = 0;
                active_file_created_new = false;
                rotated = true;
            }

            if active_file.is_none() {
                match open_wal_segment_for_append(&active_segment_path).await {
                    Ok((file, len, created_new)) => {
                        active_file = Some(file);
                        active_len = len;
                        active_file_created_new = created_new;
                    }
                    Err(e) => {
                        batch_failed = Some(e.to_string());
                        let _ = request.response.send(Err(AppError::InternalServerError(
                            batch_failed.clone().unwrap(),
                        )));
                        break;
                    }
                }
            }

            if let Some(file) = active_file.as_mut() {
                if let Err(e) = file.write_all(&frame).await.map_err(|err| {
                    AppError::Io(err, format!("append wal {}", active_segment_path.display()))
                }) {
                    batch_failed = Some(e.to_string());
                    let _ = request.response.send(Err(AppError::InternalServerError(
                        batch_failed.clone().unwrap(),
                    )));
                    break;
                }
            }

            let sequence = runtime_guard.manifest.next_sequence;
            runtime_guard.manifest.next_sequence += 1;
            active_len = active_len.saturating_add(frame_len);
            apply_wal_record_to_materialized(&mut runtime_guard.materialized, &request.record);
            wrote_current_segment = true;
            replies.push((request.response, sequence));
        }

        if batch_failed.is_some() {
            drop(runtime_guard);
            let message = batch_failed.unwrap();
            for (response, _) in replies.drain(..) {
                let _ = response.send(Err(AppError::InternalServerError(message.clone())));
            }
            continue;
        }

        if wrote_current_segment {
            if let Some(file) = active_file.as_mut() {
                if let Err(e) =
                    flush_wal_segment(file, &active_segment_path, active_file_created_new).await
                {
                    drop(runtime_guard);
                    let message = e.to_string();
                    for (response, _) in replies.drain(..) {
                        let _ = response.send(Err(AppError::InternalServerError(message.clone())));
                    }
                    continue;
                }
            }
            active_file_created_new = false;
        }

        INDEX_WAL_MANIFEST_CACHE.lock().await.insert(
            wal_manifest_cache_key(&root, &fragment, partition_id),
            runtime_guard.manifest.clone(),
        );
        let persisted_sequence = runtime_guard.manifest.next_sequence.saturating_sub(1);
        let needs_maintenance = rotated
            || persisted_sequence == 1
            || persisted_sequence % INDEX_WAL_MANIFEST_PERSIST_INTERVAL == 0
            || persisted_sequence.saturating_sub(runtime_guard.manifest.last_checkpoint_sequence)
                >= INDEX_CHECKPOINT_RECORD_INTERVAL;
        drop(runtime_guard);
        for (response, sequence) in replies.drain(..) {
            let _ = response.send(Ok(sequence));
        }
        if needs_maintenance {
            schedule_partition_maintenance(root.clone(), fragment.clone(), partition_id);
        }
    }
}

fn decode_length_prefixed_records<T>(bytes: &[u8], path: &Path) -> Result<Vec<T>, AppError>
where
    T: DeserializeOwned,
{
    let mut out = Vec::new();
    let mut cursor = 0usize;
    while cursor + 4 <= bytes.len() {
        let len = u32::from_le_bytes([
            bytes[cursor],
            bytes[cursor + 1],
            bytes[cursor + 2],
            bytes[cursor + 3],
        ]) as usize;
        cursor += 4;
        if cursor + len > bytes.len() {
            return Err(AppError::InternalServerError(format!(
                "truncated wal segment {}",
                path.display()
            )));
        }
        let record = rmp_serde::from_slice::<T>(&bytes[cursor..cursor + len]).map_err(|e| {
            AppError::InternalServerError(format!("decode wal {}: {}", path.display(), e))
        })?;
        out.push(record);
        cursor += len;
    }
    if cursor != bytes.len() {
        return Err(AppError::InternalServerError(format!(
            "wal segment trailing bytes {}",
            path.display()
        )));
    }
    Ok(out)
}

fn wal_manifest_cache_key(root: &Path, fragment: &str, partition_id: u16) -> String {
    format!("{}:{}:{:04x}", root.display(), fragment, partition_id)
}

fn parse_wal_segment_id(path: &Path) -> Option<u64> {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .and_then(|stem| stem.parse::<u64>().ok())
}

async fn rebuild_wal_manifest(
    root: &Path,
    fragment: &str,
    partition_id: u16,
) -> Result<IndexWalManifest, AppError> {
    let checkpoint_up_to = read_messagepack_if_exists::<IndexCheckpoint>(&latest_checkpoint_path(
        root,
        fragment,
        partition_id,
    ))
    .await?
    .map(|checkpoint| checkpoint.up_to_sequence)
    .unwrap_or(0);
    let segments_dir = wal_segments_dir(root, fragment, partition_id);
    if !fs::try_exists(&segments_dir).await.unwrap_or(false) {
        return Ok(IndexWalManifest {
            active_segment_id: 1,
            next_sequence: checkpoint_up_to.saturating_add(1),
            last_checkpoint_sequence: checkpoint_up_to,
        });
    }

    let mut highest_segment_id = 1u64;
    let mut highest_sequence = checkpoint_up_to;
    let mut entries = match fs::read_dir(&segments_dir).await {
        Ok(entries) => entries,
        Err(_) => {
            return Ok(IndexWalManifest {
                active_segment_id: 1,
                next_sequence: checkpoint_up_to.saturating_add(1),
                last_checkpoint_sequence: checkpoint_up_to,
            });
        }
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("wal") {
            continue;
        }
        if let Some(segment_id) = parse_wal_segment_id(&path) {
            highest_segment_id = highest_segment_id.max(segment_id);
        }
        let bytes = fs::read(&path)
            .await
            .map_err(|e| AppError::Io(e, format!("read {}", path.display())))?;
        let records = decode_length_prefixed_records::<IndexWalEnvelope>(&bytes, &path)?;
        if let Some(sequence) = records.last().map(|record| record.sequence) {
            highest_sequence = highest_sequence.max(sequence);
        }
    }

    Ok(IndexWalManifest {
        active_segment_id: highest_segment_id,
        next_sequence: highest_sequence.saturating_add(1),
        last_checkpoint_sequence: checkpoint_up_to,
    })
}

async fn load_wal_manifest(
    root: &Path,
    fragment: &str,
    partition_id: u16,
) -> Result<IndexWalManifest, AppError> {
    let cache_key = wal_manifest_cache_key(root, fragment, partition_id);
    if let Some(manifest) = INDEX_WAL_MANIFEST_CACHE
        .lock()
        .await
        .get(&cache_key)
        .cloned()
    {
        return Ok(manifest);
    }

    let manifest = rebuild_wal_manifest(root, fragment, partition_id).await?;
    INDEX_WAL_MANIFEST_CACHE
        .lock()
        .await
        .insert(cache_key, manifest.clone());
    Ok(manifest)
}

async fn load_partition_runtime_state(
    root: &Path,
    fragment: &str,
    partition_id: u16,
    manifest: &IndexWalManifest,
) -> Result<PartitionRuntimeState, AppError> {
    let checkpoint = read_messagepack_if_exists::<IndexCheckpoint>(&latest_checkpoint_path(
        root,
        fragment,
        partition_id,
    ))
    .await?;
    let after_sequence = checkpoint
        .as_ref()
        .map(|value| value.up_to_sequence)
        .unwrap_or(0);
    let mut materialized = if let Some(checkpoint) = checkpoint {
        checkpoint_to_materialized(checkpoint)
    } else {
        PartitionMaterializedState {
            live_latest: collect_partition_latest_states_on_root(
                root,
                fragment,
                partition_id,
                false,
            )
            .await?
            .into_iter()
            .map(|state| (canonical_key(&state.metadata.object_key), state))
            .collect(),
            trash_latest: collect_partition_latest_states_on_root(
                root,
                fragment,
                partition_id,
                true,
            )
            .await?
            .into_iter()
            .map(|state| (canonical_key(&state.metadata.object_key), state))
            .collect(),
            sessions: materialize_partition_sessions_on_root(root, fragment, partition_id)
                .await?
                .into_iter()
                .map(|state| (state.manifest.version_id.clone(), state))
                .collect(),
        }
    };
    for envelope in read_wal_records_since(root, fragment, partition_id, after_sequence).await? {
        apply_wal_record_to_materialized(&mut materialized, &envelope.record);
    }
    let mut manifest = manifest.clone();
    if manifest.last_checkpoint_sequence < after_sequence {
        manifest.last_checkpoint_sequence = after_sequence;
    }
    Ok(PartitionRuntimeState {
        manifest,
        materialized,
    })
}

async fn persist_wal_manifest(
    root: &Path,
    fragment: &str,
    partition_id: u16,
    manifest: &IndexWalManifest,
) -> Result<(), AppError> {
    let bytes = rmp_serde::to_vec(manifest)
        .map_err(|e| AppError::InternalServerError(format!("serialize wal manifest: {}", e)))?;
    write_atomic(&wal_manifest_path(root, fragment, partition_id), &bytes).await
}

async fn collect_partition_latest_states_on_root(
    root: &Path,
    fragment: &str,
    partition_id: u16,
    trashed: bool,
) -> Result<Vec<IndexLatestState>, AppError> {
    let mut out = Vec::new();
    let objects_root = partition_dir(root, fragment, partition_id).join(if trashed {
        INDEX_TRASH_DIR
    } else {
        INDEX_OBJECTS_DIR
    });
    if !fs::try_exists(&objects_root).await.unwrap_or(false) {
        return Ok(out);
    }

    let mut bucket_dirs = match fs::read_dir(&objects_root).await {
        Ok(entries) => entries,
        Err(_) => return Ok(out),
    };
    while let Ok(Some(bucket_dir)) = bucket_dirs.next_entry().await {
        let mut object_dirs = match fs::read_dir(bucket_dir.path()).await {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        while let Ok(Some(object_dir)) = object_dirs.next_entry().await {
            let latest_path = object_dir.path().join(latest_marker_name());
            if !fs::try_exists(&latest_path).await.unwrap_or(false) {
                continue;
            }
            match read_messagepack::<IndexLatestState>(&latest_path).await {
                Ok(state) => out.push(state),
                Err(e) => warn!(
                    "index checkpoint latest decode failed {}: {}",
                    latest_path.display(),
                    e
                ),
            }
        }
    }

    Ok(out)
}

async fn maybe_write_checkpoint(
    root: &Path,
    fragment: &str,
    partition_id: u16,
    manifest: &mut IndexWalManifest,
) -> Result<bool, AppError> {
    let current_sequence = manifest.next_sequence.saturating_sub(1);
    if current_sequence == 0
        || current_sequence.saturating_sub(manifest.last_checkpoint_sequence)
            < INDEX_CHECKPOINT_RECORD_INTERVAL
    {
        return Ok(false);
    }

    let live_latest =
        collect_partition_latest_states_on_root(root, fragment, partition_id, false).await?;
    let trash_latest =
        collect_partition_latest_states_on_root(root, fragment, partition_id, true).await?;
    let sessions = materialize_partition_sessions_on_root(root, fragment, partition_id).await?;
    let checkpoint = IndexCheckpoint {
        created_at: Utc::now().timestamp(),
        up_to_sequence: current_sequence,
        live_latest,
        trash_latest,
        sessions,
    };
    let bytes = rmp_serde::to_vec(&checkpoint)
        .map_err(|e| AppError::InternalServerError(format!("serialize checkpoint: {}", e)))?;
    write_atomic(
        &latest_checkpoint_path(root, fragment, partition_id),
        &bytes,
    )
    .await?;
    manifest.last_checkpoint_sequence = current_sequence;
    Ok(true)
}

async fn append_wal(
    root: &Path,
    fragment: &str,
    partition_id: u16,
    record: &IndexWalRecord,
) -> Result<u64, AppError> {
    let sender = get_wal_appender(root, fragment, partition_id).await;
    let (response_tx, response_rx) = oneshot::channel();
    sender
        .send(WalAppendRequest {
            record: record.clone(),
            response: response_tx,
        })
        .await
        .map_err(|_| {
            AppError::InternalServerError(format!(
                "wal appender unavailable {}:{}:{:04x}",
                root.display(),
                fragment,
                partition_id
            ))
        })?;
    response_rx.await.map_err(|_| {
        AppError::InternalServerError(format!(
            "wal appender dropped {}:{}:{:04x}",
            root.display(),
            fragment,
            partition_id
        ))
    })?
}

async fn read_current_checkpoint(
    root: &Path,
    fragment: &str,
    partition_id: u16,
) -> Result<Option<IndexCheckpoint>, AppError> {
    let Some(checkpoint) = read_messagepack_if_exists::<IndexCheckpoint>(&latest_checkpoint_path(
        root,
        fragment,
        partition_id,
    ))
    .await?
    else {
        return Ok(None);
    };
    if read_wal_records_since(root, fragment, partition_id, checkpoint.up_to_sequence)
        .await?
        .is_empty()
    {
        return Ok(Some(checkpoint));
    }
    Ok(None)
}

async fn read_wal_records_since(
    root: &Path,
    fragment: &str,
    partition_id: u16,
    after_sequence: u64,
) -> Result<Vec<IndexWalEnvelope>, AppError> {
    let segments_dir = wal_segments_dir(root, fragment, partition_id);
    if !fs::try_exists(&segments_dir).await.unwrap_or(false) {
        return Ok(Vec::new());
    }

    let mut segment_entries = match fs::read_dir(&segments_dir).await {
        Ok(entries) => entries,
        Err(_) => return Ok(Vec::new()),
    };
    let mut segment_paths = Vec::new();
    while let Ok(Some(entry)) = segment_entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".wal") {
            segment_paths.push((name, entry.path()));
        }
    }
    segment_paths.sort_by(|left, right| left.0.cmp(&right.0));

    let mut out = Vec::new();
    for (_name, path) in segment_paths {
        let bytes = fs::read(&path)
            .await
            .map_err(|e| AppError::Io(e, format!("read {}", path.display())))?;
        let records = decode_length_prefixed_records::<IndexWalEnvelope>(&bytes, &path)?;
        out.extend(
            records
                .into_iter()
                .filter(|record| record.sequence > after_sequence),
        );
    }
    Ok(out)
}

fn apply_session_wal_record(
    sessions: &mut HashMap<String, IndexSessionState>,
    envelope: &IndexWalEnvelope,
) {
    match &envelope.record {
        IndexWalRecord::SessionOpened { session } => {
            sessions.insert(
                session.manifest.version_id.clone(),
                normalize_session_state(session.clone()),
            );
        }
        IndexWalRecord::PartCommitted {
            fragment,
            version_id,
            part_index,
            part,
            committed_at,
        } => {
            if let Some(state) = sessions.get_mut(version_id) {
                if state.manifest.fragment == *fragment {
                    if let Some(materialized_part) =
                        materialize_session_part_record(&state.manifest, *part_index, part)
                    {
                        state.parts.insert(*part_index, materialized_part);
                        state.last_activity_at = *committed_at;
                    }
                }
            }
        }
        IndexWalRecord::SessionRemoved {
            fragment,
            version_id,
            removed_at,
            ..
        } => {
            if let Some(state) = sessions.get_mut(version_id) {
                if state.manifest.fragment == *fragment {
                    state.aborted_at = Some(*removed_at);
                    state.last_activity_at = *removed_at;
                }
            }
            sessions.remove(version_id);
        }
        _ => {}
    }
}

async fn load_session_parts_on_root(
    root: &Path,
    fragment: &str,
    partition_id: u16,
    version_id: &str,
) -> Result<BTreeMap<u64, PartMetadata>, AppError> {
    let dir = session_parts_dir(root, fragment, partition_id, version_id);
    if !fs::try_exists(&dir).await.unwrap_or(false) {
        return Ok(BTreeMap::new());
    }

    let mut parts = BTreeMap::new();
    let mut entries = match fs::read_dir(&dir).await {
        Ok(entries) => entries,
        Err(e)
            if e.kind() == std::io::ErrorKind::NotFound
                || e.kind() == std::io::ErrorKind::NotADirectory =>
        {
            return Ok(parts);
        }
        Err(e) => return Err(AppError::Io(e, format!("read_dir {}", dir.display()))),
    };

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("msgpack") {
            continue;
        }
        match read_messagepack_if_exists::<PartMetadata>(&path).await {
            Ok(Some(part)) => {
                parts.insert(part.part_index, part);
            }
            Ok(None) => {}
            Err(e) => {
                warn!("index session part decode failed {}: {}", path.display(), e);
            }
        }
    }

    Ok(parts)
}

async fn enrich_session_state_from_root(
    root: &Path,
    fragment: &str,
    partition_id: u16,
    state: &mut IndexSessionState,
) -> Result<(), AppError> {
    let parts =
        load_session_parts_on_root(root, fragment, partition_id, &state.manifest.version_id).await?;
    if !parts.is_empty() {
        let latest_part_activity = parts
            .values()
            .map(|part| part.created_at)
            .max()
            .unwrap_or(state.last_activity_at);
        state.parts.extend(parts);
        state.last_activity_at = state.last_activity_at.max(latest_part_activity);
    }
    Ok(())
}

async fn materialize_partition_sessions_on_root(
    root: &Path,
    fragment: &str,
    partition_id: u16,
) -> Result<Vec<IndexSessionState>, AppError> {
    let checkpoint = read_messagepack_if_exists::<IndexCheckpoint>(&latest_checkpoint_path(
        root,
        fragment,
        partition_id,
    ))
    .await?;
    let after_sequence = checkpoint
        .as_ref()
        .map(|value| value.up_to_sequence)
        .unwrap_or(0);
    let mut sessions = checkpoint
        .map(|value| {
            value
                .sessions
                .into_iter()
                .map(normalize_session_state)
                .map(|state| (state.manifest.version_id.clone(), state))
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default();
    for envelope in read_wal_records_since(root, fragment, partition_id, after_sequence).await? {
        apply_session_wal_record(&mut sessions, &envelope);
    }

    let mut out = Vec::with_capacity(sessions.len());
    for mut state in sessions.into_values() {
        enrich_session_state_from_root(root, fragment, partition_id, &mut state).await?;
        out.push(normalize_session_state(state));
    }
    Ok(out)
}

async fn maybe_replicate<F>(label: &'static str, fut: F)
where
    F: std::future::Future<Output = Result<(), AppError>> + Send + 'static,
{
    #[cfg(test)]
    {
        if let Err(e) = fut.await {
            warn!("{} failed: {}", label, e);
        }
    }
    #[cfg(not(test))]
    {
        tokio::spawn(async move {
            let _permit = INDEX_REPLICA_IO_GATE.acquire().await.ok();
            if let Err(e) = fut.await {
                warn!("{} failed: {}", label, e);
            }
        });
    }
}

async fn persist_session_snapshot_on_roots(
    route: &IndexRoute,
    fragment: &str,
    version_id: &str,
    state: &IndexSessionState,
    replica_write_mode: IndexReplicaWriteMode,
) -> Result<(), AppError> {
    let bytes = rmp_serde::to_vec(state)
        .map_err(|e| AppError::InternalServerError(format!("serialize session snapshot: {}", e)))?;
    let owner_root = route.roots[0].clone();
    let owner_path = session_state_path(&owner_root, fragment, route.partition_id, version_id);
    write_atomic(&owner_path, &bytes).await?;

    if route.roots.len() > 1 {
        let replica_roots = route.roots[1..].to_vec();
        let fragment_clone = fragment.to_string();
        let version_clone = version_id.to_string();
        let bytes_clone = bytes.clone();
        let partition_id = route.partition_id;
        let replicate_fut = async move {
            for root in replica_roots {
                let path =
                    session_state_path(&root, &fragment_clone, partition_id, &version_clone);
                write_atomic(&path, &bytes_clone).await?;
            }
            Ok(())
        };
        match replica_write_mode {
            IndexReplicaWriteMode::BackgroundBestEffort => {
                maybe_replicate("index session snapshot replica sync", replicate_fut).await;
            }
            IndexReplicaWriteMode::SyncBestEffort => {
                if let Err(e) = replicate_fut.await {
                    warn!(
                        "index session snapshot replica sync failed fragment={} version={}: {}",
                        fragment, version_id, e
                    );
                }
            }
        }
    }

    Ok(())
}

async fn persist_removed_session_receipt_on_roots(
    route: &IndexRoute,
    fragment: &str,
    version_id: &str,
    receipt: &RemovedSessionReceipt,
    replica_write_mode: IndexReplicaWriteMode,
) -> Result<(), AppError> {
    let bytes = rmp_serde::to_vec(receipt)
        .map_err(|e| AppError::InternalServerError(format!("serialize removed session receipt: {}", e)))?;
    let owner_root = route.roots[0].clone();
    let owner_path =
        session_removed_receipt_path(&owner_root, fragment, route.partition_id, version_id);
    write_atomic(&owner_path, &bytes).await?;

    if route.roots.len() > 1 {
        let replica_roots = route.roots[1..].to_vec();
        let fragment_clone = fragment.to_string();
        let version_clone = version_id.to_string();
        let bytes_clone = bytes.clone();
        let partition_id = route.partition_id;
        let replicate_fut = async move {
            for root in replica_roots {
                let path = session_removed_receipt_path(
                    &root,
                    &fragment_clone,
                    partition_id,
                    &version_clone,
                );
                write_atomic(&path, &bytes_clone).await?;
            }
            Ok(())
        };
        match replica_write_mode {
            IndexReplicaWriteMode::BackgroundBestEffort => {
                maybe_replicate("index removed session receipt replica sync", replicate_fut).await;
            }
            IndexReplicaWriteMode::SyncBestEffort => {
                if let Err(e) = replicate_fut.await {
                    warn!(
                        "index removed session receipt replica sync failed fragment={} version={}: {}",
                        fragment, version_id, e
                    );
                }
            }
        }
    }

    Ok(())
}

#[cfg(not(test))]
fn partition_maintenance_task_key(root: &Path, fragment: &str, partition_id: u16) -> String {
    format!("{}:{}:{:04x}", root.display(), fragment, partition_id)
}

async fn run_partition_maintenance(
    root: &Path,
    fragment: &str,
    partition_id: u16,
) -> Result<(), AppError> {
    let key = runtime_key(root, fragment, partition_id);
    if let Some(runtime) = INDEX_PARTITION_RUNTIME.lock().await.get(&key).cloned() {
        let (mut manifest, checkpoint) = {
            let runtime = runtime.lock().await;
            let checkpoint = materialized_to_checkpoint(
                &runtime.materialized,
                runtime.manifest.next_sequence.saturating_sub(1),
            );
            (runtime.manifest.clone(), checkpoint)
        };
        let current_sequence = checkpoint.up_to_sequence;
        let checkpoint_written = if current_sequence > 0
            && current_sequence.saturating_sub(manifest.last_checkpoint_sequence)
                >= INDEX_CHECKPOINT_RECORD_INTERVAL
        {
            let bytes = rmp_serde::to_vec(&checkpoint).map_err(|e| {
                AppError::InternalServerError(format!("serialize checkpoint: {}", e))
            })?;
            write_atomic(
                &latest_checkpoint_path(root, fragment, partition_id),
                &bytes,
            )
            .await?;
            manifest.last_checkpoint_sequence = current_sequence;
            true
        } else {
            false
        };
        let persisted_sequence = manifest.next_sequence.saturating_sub(1);
        if checkpoint_written
            || persisted_sequence == 1
            || persisted_sequence % INDEX_WAL_MANIFEST_PERSIST_INTERVAL == 0
        {
            persist_wal_manifest(root, fragment, partition_id, &manifest).await?;
        }
        {
            let mut runtime = runtime.lock().await;
            runtime.manifest.last_checkpoint_sequence = runtime
                .manifest
                .last_checkpoint_sequence
                .max(manifest.last_checkpoint_sequence);
        }
        INDEX_WAL_MANIFEST_CACHE.lock().await.insert(
            wal_manifest_cache_key(root, fragment, partition_id),
            manifest,
        );
        return Ok(());
    }

    let mut manifest = load_wal_manifest(root, fragment, partition_id).await?;
    let checkpoint_written =
        maybe_write_checkpoint(root, fragment, partition_id, &mut manifest).await?;
    let persisted_sequence = manifest.next_sequence.saturating_sub(1);
    if checkpoint_written
        || persisted_sequence == 1
        || persisted_sequence % INDEX_WAL_MANIFEST_PERSIST_INTERVAL == 0
    {
        persist_wal_manifest(root, fragment, partition_id, &manifest).await?;
    }
    INDEX_WAL_MANIFEST_CACHE.lock().await.insert(
        wal_manifest_cache_key(root, fragment, partition_id),
        manifest,
    );
    Ok(())
}

#[cfg(test)]
fn schedule_partition_maintenance(root: PathBuf, fragment: String, partition_id: u16) {
    tokio::task::block_in_place(|| {
        let handle = tokio::runtime::Handle::current();
        handle.block_on(async move {
            if let Err(e) = run_partition_maintenance(&root, &fragment, partition_id).await {
                warn!(
                    "index partition maintenance failed root={} fragment={} partition={:04x}: {}",
                    root.display(),
                    fragment,
                    partition_id,
                    e
                );
            }
        });
    });
}

#[cfg(not(test))]
fn schedule_partition_maintenance(root: PathBuf, fragment: String, partition_id: u16) {
    tokio::spawn(async move {
        let task_key = partition_maintenance_task_key(&root, &fragment, partition_id);
        let should_start = {
            let mut tasks = INDEX_PARTITION_MAINTENANCE_TASKS.lock().await;
            let state = tasks.entry(task_key.clone()).or_default();
            if state.running {
                state.rerun_requested = true;
                false
            } else {
                state.running = true;
                state.rerun_requested = false;
                true
            }
        };
        if !should_start {
            return;
        }

        loop {
            let _permit = match INDEX_BACKGROUND_IO_GATE.acquire().await {
                Ok(permit) => permit,
                Err(_) => break,
            };
            let maintenance_result =
                run_partition_maintenance(&root, &fragment, partition_id).await;
            if let Err(e) = &maintenance_result {
                warn!(
                    "index partition maintenance failed root={} fragment={} partition={:04x}: {}",
                    root.display(),
                    fragment,
                    partition_id,
                    e
                );
            }

            let should_rerun = {
                let mut tasks = INDEX_PARTITION_MAINTENANCE_TASKS.lock().await;
                match tasks.get_mut(&task_key) {
                    Some(state) => {
                        let rerun = state.rerun_requested || maintenance_result.is_err();
                        state.rerun_requested = false;
                        if !rerun {
                            tasks.remove(&task_key);
                        }
                        rerun
                    }
                    None => false,
                }
            };
            if !should_rerun {
                break;
            }

            if maintenance_result.is_err() {
                sleep(Duration::from_millis(250)).await;
            }
        }
    });
}

fn canonical_key(key: &str) -> String {
    key.trim_start_matches('/').to_string()
}

fn canonical_prefix(prefix: &str) -> String {
    prefix.trim_matches('/').to_string()
}

fn object_key_matches_prefix(object_key: &str, prefix: &str) -> bool {
    let key = canonical_key(object_key);
    let pfx = canonical_prefix(prefix);
    if pfx.is_empty() {
        return true;
    }
    key == pfx || key.starts_with(&(pfx + "/"))
}

fn prefixes_for_key(object_key: &str) -> BTreeSet<String> {
    let key = canonical_key(object_key);
    let mut out = BTreeSet::new();
    out.insert(String::new());
    if key.is_empty() {
        return out;
    }

    let parts: Vec<&str> = key.split('/').filter(|part| !part.is_empty()).collect();
    let mut current = String::new();
    for (idx, part) in parts.iter().enumerate() {
        if idx == 0 {
            current.push_str(part);
        } else {
            current.push('/');
            current.push_str(part);
        }
        out.insert(current.clone());
    }

    out
}

async fn write_prefix_compacted_snapshot(
    path_manager: &ObjectPathManager,
    fragment: &str,
    prefix: &str,
    entries: Vec<PrefixIndexEntry>,
) -> Result<(), AppError> {
    let prefix_segment = PrefixIndexSegment {
        prefix: prefix.to_string(),
        entries,
    };
    let prefix_bytes = rmp_serde::to_vec(&prefix_segment)
        .map_err(|e| AppError::InternalServerError(format!("serialize prefix snapshot: {}", e)))?;
    let delimiter_segment = build_delimiter_compacted_from_entries(prefix, &prefix_segment.entries);
    let delimiter_bytes = rmp_serde::to_vec(&delimiter_segment).map_err(|e| {
        AppError::InternalServerError(format!("serialize delimiter snapshot: {}", e))
    })?;

    let bucket_dirs = path_manager.get_all_prefix_bucket_dirs(fragment, prefix);
    let compacted_paths = path_manager.get_all_prefix_compacted_paths(fragment, prefix);
    let delimiter_paths = path_manager.get_all_prefix_delimiter_compacted_paths(fragment, prefix);
    let mut wrote_any = false;

    for ((bucket_dir, compacted_path), delimiter_path) in bucket_dirs
        .into_iter()
        .zip(compacted_paths.into_iter())
        .zip(delimiter_paths.into_iter())
    {
        if let Some(parent) = compacted_path.parent() {
            let _ = fs::create_dir_all(parent).await;
        }
        if let Some(parent) = delimiter_path.parent() {
            let _ = fs::create_dir_all(parent).await;
        }

        let compacted_ok = write_atomic(&compacted_path, &prefix_bytes).await.is_ok();
        let delimiter_ok = write_atomic(&delimiter_path, &delimiter_bytes)
            .await
            .is_ok();
        if !(compacted_ok && delimiter_ok) {
            continue;
        }

        wrote_any = true;
        if fs::try_exists(&bucket_dir).await.unwrap_or(false) {
            let mut entries = match fs::read_dir(&bucket_dir).await {
                Ok(entries) => entries,
                Err(_) => continue,
            };
            while let Ok(Some(entry)) = entries.next_entry().await {
                let name = entry.file_name().to_string_lossy().to_string();
                if (name.starts_with("seg_") || name.starts_with(PREFIX_DELIMITER_SEGMENT_PREFIX))
                    && name.ends_with(PREFIX_SEGMENT_EXT)
                {
                    let _ = fs::remove_file(entry.path()).await;
                }
            }
            let _ = sync_dir(&bucket_dir).await;
        }
    }

    if !wrote_any {
        return Err(AppError::InternalServerError(format!(
            "failed to write prefix snapshot for prefix '{}'",
            prefix
        )));
    }
    Ok(())
}

async fn load_prefix_compacted_snapshot(
    fragment: &str,
    prefix: &str,
) -> Result<Option<PrefixIndexSegment>, AppError> {
    let path_manager = ObjectPathManager::new();
    let mut replicas = Vec::<(SystemTime, PathBuf)>::new();
    for bucket_dir in path_manager.get_all_prefix_bucket_dirs(fragment, prefix) {
        let compacted_path = bucket_dir.join(format!("compacted{}", PREFIX_SEGMENT_EXT));
        if !fs::try_exists(&compacted_path).await.unwrap_or(false) {
            continue;
        }
        let modified = fs::metadata(&compacted_path)
            .await
            .ok()
            .and_then(|meta| meta.modified().ok())
            .unwrap_or(SystemTime::UNIX_EPOCH);
        replicas.push((modified, compacted_path));
    }

    replicas.sort_by(|left, right| right.0.cmp(&left.0));
    for (_modified, path) in replicas {
        match read_messagepack::<PrefixIndexSegment>(&path).await {
            Ok(segment) if canonical_prefix(&segment.prefix) == canonical_prefix(prefix) => {
                return Ok(Some(segment));
            }
            Ok(_) => continue,
            Err(e) => warn!(
                "index prefix snapshot read failed {}: {}",
                path.display(),
                e
            ),
        }
    }

    Ok(None)
}

async fn load_prefix_projection_cursor(fragment: &str) -> Result<PrefixProjectionCursor, AppError> {
    let roots = ObjectPathManager::new().get_all_index_roots();
    let mut replicas = Vec::<(PathBuf, SystemTime)>::new();
    for root in roots {
        let path = prefix_projection_cursor_path(&root, fragment);
        if !fs::try_exists(&path).await.unwrap_or(false) {
            continue;
        }
        let modified = fs::metadata(&path)
            .await
            .ok()
            .and_then(|meta| meta.modified().ok())
            .unwrap_or(SystemTime::UNIX_EPOCH);
        replicas.push((path, modified));
    }
    replicas.sort_by(|left, right| right.1.cmp(&left.1));
    for (path, _) in replicas {
        match read_messagepack::<PrefixProjectionCursor>(&path).await {
            Ok(cursor) => return Ok(cursor),
            Err(e) => warn!("index prefix cursor read failed {}: {}", path.display(), e),
        }
    }
    Ok(PrefixProjectionCursor::default())
}

async fn persist_prefix_projection_cursor(
    fragment: &str,
    cursor: &PrefixProjectionCursor,
) -> Result<(), AppError> {
    let roots = ObjectPathManager::new().get_all_index_roots();
    let bytes = rmp_serde::to_vec(cursor).map_err(|e| {
        AppError::InternalServerError(format!("serialize prefix projection cursor: {}", e))
    })?;
    let mut success = 0usize;
    for root in roots {
        let path = prefix_projection_cursor_path(&root, fragment);
        match write_atomic(&path, &bytes).await {
            Ok(()) => success += 1,
            Err(e) => warn!(
                "index prefix cursor persist failed {}: {}",
                path.display(),
                e
            ),
        }
    }
    if success == 0 {
        return Err(AppError::InternalServerError(
            "failed to persist prefix projection cursor".to_string(),
        ));
    }
    Ok(())
}

async fn project_fragment_prefixes(
    fragment: &str,
    mut changed_keys: HashSet<String>,
    recover_from_wal: bool,
) -> Result<(), AppError> {
    let path_manager = ObjectPathManager::new();
    let roots = path_manager.get_all_index_roots();
    let mut cursor = if recover_from_wal {
        load_prefix_projection_cursor(fragment).await?
    } else {
        PrefixProjectionCursor::default()
    };

    if recover_from_wal {
        for root in roots {
            let partitions_root = root
                .join(fragment)
                .join(INDEX_DIR)
                .join(INDEX_PARTITIONS_DIR);
            if !fs::try_exists(&partitions_root).await.unwrap_or(false) {
                continue;
            }

            let mut partitions = match fs::read_dir(&partitions_root).await {
                Ok(entries) => entries,
                Err(_) => continue,
            };
            while let Ok(Some(partition)) = partitions.next_entry().await {
                let partition_name = partition.file_name().to_string_lossy().to_string();
                let Ok(partition_id) = u16::from_str_radix(&partition_name, 16) else {
                    continue;
                };
                let source_id = format!("{}::{:04x}", root.display(), partition_id);
                let after_sequence = cursor.sources.get(&source_id).copied().unwrap_or(0);
                let records = match read_wal_records_since(
                    &root,
                    fragment,
                    partition_id,
                    after_sequence,
                )
                .await
                {
                    Ok(records) => records,
                    Err(e) => {
                        warn!(
                            "index prefix projector wal read failed fragment={} root={} partition={:04x}: {}",
                            fragment,
                            root.display(),
                            partition_id,
                            e
                        );
                        continue;
                    }
                };
                let mut max_sequence = after_sequence;
                for envelope in records {
                    max_sequence = max_sequence.max(envelope.sequence);
                    match envelope.record {
                        IndexWalRecord::VersionPublished { metadata, .. }
                        | IndexWalRecord::VersionTrashed { metadata, .. } => {
                            changed_keys.insert(canonical_key(&metadata.object_key));
                        }
                        IndexWalRecord::VersionRemoved { object_key, .. } => {
                            changed_keys.insert(canonical_key(&object_key));
                        }
                        IndexWalRecord::SessionOpened { .. }
                        | IndexWalRecord::PartCommitted { .. }
                        | IndexWalRecord::SessionRemoved { .. } => {}
                    }
                }
                if max_sequence > after_sequence {
                    cursor.sources.insert(source_id, max_sequence);
                }
            }
        }
    }

    if changed_keys.is_empty() {
        return if recover_from_wal {
            persist_prefix_projection_cursor(fragment, &cursor).await
        } else {
            Ok(())
        };
    }

    let affected_prefixes = changed_keys
        .iter()
        .flat_map(|key| prefixes_for_key(key))
        .map(|prefix| canonical_prefix(&prefix))
        .collect::<BTreeSet<_>>();
    let mut live_entries = HashMap::<String, Option<PrefixIndexEntry>>::new();
    for key in &changed_keys {
        let current = match read_latest_object(fragment, key).await {
            Ok(metadata) if !metadata.is_delete_marker => {
                Some(PrefixIndexEntry::from_metadata(&metadata))
            }
            Ok(_) => None,
            Err(AppError::NotFound) => None,
            Err(e) => {
                warn!(
                    "index prefix projection latest read failed fragment={} key={}: {}",
                    fragment, key, e
                );
                return Err(e);
            }
        };
        live_entries.insert(key.clone(), current);
    }

    let mut write_errors = 0usize;
    for prefix in affected_prefixes {
        let mut latest_by_key = match load_prefix_compacted_snapshot(fragment, &prefix).await? {
            Some(snapshot) => snapshot
                .entries
                .into_iter()
                .map(|mut entry| {
                    entry.object_key = canonical_key(&entry.object_key);
                    (entry.object_key.clone(), entry)
                })
                .collect::<HashMap<_, _>>(),
            None => list_fragment_objects_for_prefix(fragment, &prefix)
                .await?
                .into_iter()
                .filter(|metadata| !metadata.is_delete_marker)
                .map(|metadata| {
                    let entry = PrefixIndexEntry::from_metadata(&metadata);
                    (entry.object_key.clone(), entry)
                })
                .collect::<HashMap<_, _>>(),
        };

        for key in &changed_keys {
            if !object_key_matches_prefix(key, &prefix) {
                continue;
            }
            latest_by_key.remove(key);
            if let Some(Some(entry)) = live_entries.get(key) {
                latest_by_key.insert(key.clone(), entry.clone());
            }
        }

        let mut entries = latest_by_key.into_values().collect::<Vec<_>>();
        entries.sort_by(|left, right| {
            left.object_key
                .cmp(&right.object_key)
                .then(left.version_id.cmp(&right.version_id))
        });

        if let Err(e) =
            write_prefix_compacted_snapshot(&path_manager, fragment, &prefix, entries).await
        {
            write_errors += 1;
            warn!(
                "index prefix projection write failed fragment={} prefix='{}': {}",
                fragment, prefix, e
            );
        }
    }

    if write_errors > 0 {
        return Err(AppError::InternalServerError(format!(
            "prefix projection failed for {} prefixes",
            write_errors
        )));
    }

    if recover_from_wal {
        persist_prefix_projection_cursor(fragment, &cursor).await
    } else {
        Ok(())
    }
}

#[cfg(test)]
fn schedule_prefix_projection_internal(
    fragment: String,
    object_key: Option<String>,
    recover_from_wal: bool,
) {
    let mut changed_keys = HashSet::new();
    if let Some(object_key) = object_key {
        changed_keys.insert(canonical_key(&object_key));
    }
    tokio::task::block_in_place(|| {
        let handle = tokio::runtime::Handle::current();
        handle.block_on(async move {
            if let Err(e) =
                project_fragment_prefixes(&fragment, changed_keys, recover_from_wal).await
            {
                warn!(
                    "index prefix projection task failed fragment={}: {}",
                    fragment, e
                );
            }
        });
    });
}

#[cfg(not(test))]
fn schedule_prefix_projection_internal(
    fragment: String,
    object_key: Option<String>,
    recover_from_wal: bool,
) {
    tokio::spawn(async move {
        let should_start = {
            let mut tasks = PREFIX_PROJECTION_TASKS.lock().await;
            let state = tasks.entry(fragment.clone()).or_default();
            if let Some(object_key) = object_key.as_ref() {
                state.dirty_keys.insert(canonical_key(object_key));
            }
            if recover_from_wal {
                state.task.recovery_scan_needed = true;
            }
            if state.task.running {
                state.task.rerun_requested = true;
                false
            } else {
                state.task.running = true;
                state.task.rerun_requested = false;
                true
            }
        };
        if !should_start {
            return;
        }

        loop {
            let _permit = match PREFIX_PROJECTION_IO_GATE.acquire().await {
                Ok(permit) => permit,
                Err(_) => break,
            };
            let (dirty_keys, recovery_scan_needed) = {
                let mut tasks = PREFIX_PROJECTION_TASKS.lock().await;
                let state = tasks.entry(fragment.clone()).or_default();
                let dirty_keys = std::mem::take(&mut state.dirty_keys);
                let recovery_scan_needed = state.task.recovery_scan_needed;
                state.task.recovery_scan_needed = false;
                (dirty_keys, recovery_scan_needed)
            };
            let projection_result =
                project_fragment_prefixes(&fragment, dirty_keys.clone(), recovery_scan_needed)
                    .await;
            if let Err(e) = &projection_result {
                warn!(
                    "index prefix projection task failed fragment={}: {}",
                    fragment, e
                );
            }

            let should_rerun = {
                let mut tasks = PREFIX_PROJECTION_TASKS.lock().await;
                match tasks.get_mut(&fragment) {
                    Some(state) => {
                        if projection_result.is_err() {
                            state.dirty_keys.extend(dirty_keys);
                            state.task.recovery_scan_needed |= recovery_scan_needed;
                        }
                        let rerun = state.task.rerun_requested || projection_result.is_err();
                        state.task.rerun_requested = false;
                        if !rerun {
                            tasks.remove(&fragment);
                        }
                        rerun
                    }
                    None => false,
                }
            };
            if !should_rerun {
                break;
            }

            if projection_result.is_err() {
                sleep(Duration::from_millis(250)).await;
            }
        }
    });
}

fn schedule_prefix_projection_for_key(fragment: String, object_key: String) {
    schedule_prefix_projection_internal(fragment, Some(object_key), false);
}

pub async fn is_prefix_projection_pending(fragment: &str) -> bool {
    PREFIX_PROJECTION_TASKS
        .lock()
        .await
        .get(fragment)
        .map(|state| state.task.running || state.task.rerun_requested)
        .unwrap_or(false)
}

fn metadata_is_newer(candidate: &ObjectMetadata, current: &ObjectMetadata) -> bool {
    if version_recency_is_newer(
        candidate.created_at,
        &candidate.version_id,
        current.created_at,
        &current.version_id,
    ) {
        return true;
    }

    if candidate.created_at == current.created_at && candidate.version_id == current.version_id {
        return metadata_replica_rank(candidate) > metadata_replica_rank(current);
    }

    false
}

fn metadata_replica_rank(metadata: &ObjectMetadata) -> (usize, usize, usize, usize) {
    let total_shards = metadata.parts.iter().map(|part| part.shards.len()).sum();
    let raw_shards = metadata
        .parts
        .iter()
        .flat_map(|part| part.shards.iter())
        .filter(|shard| shard.raw_blob_address.is_some())
        .count();

    (
        metadata.parts.len(),
        total_shards,
        raw_shards,
        metadata.stripe_size_bytes,
    )
}

fn latest_marker_name() -> String {
    format!("latest{}", INDEX_LATEST_SUFFIX)
}

fn is_version_entry(name: &str) -> bool {
    name.ends_with(INDEX_OBJECT_SUFFIX) && name != latest_marker_name()
}

async fn load_latest_state_for_state(
    route: &IndexRoute,
    fragment: &str,
    object_key: &str,
    trashed: bool,
) -> Option<IndexLatestState> {
    let mut replicas = Vec::<(PathBuf, SystemTime)>::new();
    for root in &route.roots {
        let path =
            latest_state_path_for_state(root, fragment, route.partition_id, object_key, trashed);
        if !fs::try_exists(&path).await.unwrap_or(false) {
            continue;
        }
        let modified = fs::metadata(&path)
            .await
            .ok()
            .and_then(|meta| meta.modified().ok())
            .unwrap_or(SystemTime::UNIX_EPOCH);
        replicas.push((path, modified));
    }
    replicas.sort_by(|left, right| right.1.cmp(&left.1));
    for (path, _) in replicas {
        match read_messagepack::<IndexLatestState>(&path).await {
            Ok(state) => return Some(state),
            Err(e) => warn!("index latest read failed {}: {}", path.display(), e),
        }
    }
    None
}

async fn load_latest_state(
    route: &IndexRoute,
    fragment: &str,
    object_key: &str,
) -> Option<IndexLatestState> {
    load_latest_state_for_state(route, fragment, object_key, false).await
}

async fn collect_object_versions_for_state(
    route: &IndexRoute,
    fragment: &str,
    object_key: &str,
    trashed: bool,
) -> Vec<ObjectMetadata> {
    let mut versions = BTreeMap::<String, ObjectMetadata>::new();
    for root in &route.roots {
        let dir =
            object_prefix_dir_for_state(root, fragment, route.partition_id, object_key, trashed);
        if !fs::try_exists(&dir).await.unwrap_or(false) {
            continue;
        }
        let mut entries = match fs::read_dir(&dir).await {
            Ok(entries) => entries,
            Err(e) => {
                warn!("index version dir read failed {}: {}", dir.display(), e);
                continue;
            }
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let file_name = entry.file_name().to_string_lossy().to_string();
            if !is_version_entry(&file_name) {
                continue;
            }
            let Ok(metadata) = read_messagepack::<ObjectMetadata>(&entry.path()).await else {
                continue;
            };
            match versions.get(&metadata.version_id) {
                None => {
                    versions.insert(metadata.version_id.clone(), metadata);
                }
                Some(current) if metadata_is_newer(&metadata, current) => {
                    versions.insert(metadata.version_id.clone(), metadata);
                }
                _ => {}
            }
        }
    }
    versions.into_values().collect()
}

async fn collect_object_versions(
    route: &IndexRoute,
    fragment: &str,
    object_key: &str,
) -> Vec<ObjectMetadata> {
    collect_object_versions_for_state(route, fragment, object_key, false).await
}

async fn persist_latest_state_for_state(
    route: &IndexRoute,
    fragment: &str,
    object_key: &str,
    latest: Option<&IndexLatestState>,
    trashed: bool,
) -> Result<(), AppError> {
    let latest_bytes = match latest {
        Some(state) => Some(
            rmp_serde::to_vec(state)
                .map_err(|e| AppError::InternalServerError(format!("serialize latest: {}", e)))?,
        ),
        None => None,
    };
    let mut success = 0usize;
    for root in &route.roots {
        let path =
            latest_state_path_for_state(root, fragment, route.partition_id, object_key, trashed);
        let result = match &latest_bytes {
            Some(bytes) => write_atomic(&path, bytes).await,
            None => {
                if fs::try_exists(&path).await.unwrap_or(false) {
                    fs::remove_file(&path)
                        .await
                        .map_err(|e| AppError::Io(e, format!("remove {}", path.display())))?;
                    if let Some(parent) = path.parent() {
                        sync_dir(parent).await?;
                    }
                }
                Ok(())
            }
        };
        match result {
            Ok(()) => success += 1,
            Err(e) => warn!(
                "index latest persist failed on root {}: {}",
                root.display(),
                e
            ),
        }
    }
    if success == 0 {
        return Err(AppError::InternalServerError(
            "index latest write failed on all roots".to_string(),
        ));
    }
    Ok(())
}

pub async fn clear_object_latest_states(fragment: &str, object_key: &str) -> Result<(), AppError> {
    let route = object_partition_route(fragment, object_key)?;
    persist_latest_state_for_state(&route, fragment, object_key, None, false).await?;
    persist_latest_state_for_state(&route, fragment, object_key, None, true).await?;
    Ok(())
}

async fn write_object_version_internal(
    metadata: &ObjectMetadata,
    force_latest: bool,
    trashed: bool,
    replica_write_mode: IndexReplicaWriteMode,
    allow_owner_failover: bool,
) -> Result<(), AppError> {
    let route = object_partition_route(&metadata.fragment, &metadata.object_key)?;
    let current_latest =
        load_latest_state_for_state(&route, &metadata.fragment, &metadata.object_key, trashed)
            .await;
    let should_update_latest = force_latest
        || current_latest
            .as_ref()
            .map(|state| {
                state.version_id == metadata.version_id
                    || metadata_is_newer(metadata, &state.metadata)
            })
            .unwrap_or(true);
    let latest = should_update_latest.then(|| IndexLatestState {
        version_id: metadata.version_id.clone(),
        metadata: metadata.clone(),
    });
    let version_bytes = rmp_serde::to_vec(metadata)
        .map_err(|e| AppError::InternalServerError(format!("serialize object: {}", e)))?;
    let latest_bytes = latest
        .as_ref()
        .map(rmp_serde::to_vec)
        .transpose()
        .map_err(|e| AppError::InternalServerError(format!("serialize latest: {}", e)))?;
    let fragment = metadata.fragment.clone();
    let wal = if trashed {
        IndexWalRecord::VersionTrashed {
            metadata: metadata.clone(),
            trashed_at: Utc::now().timestamp(),
        }
    } else {
        IndexWalRecord::VersionPublished {
            metadata: metadata.clone(),
            published_at: Utc::now().timestamp(),
        }
    };
    let partition_id = route.partition_id;
    let write_roots = if allow_owner_failover {
        let mut roots = Vec::new();
        for root in &route.roots {
            if fs::metadata(root)
                .await
                .map(|meta| meta.is_dir())
                .unwrap_or(false)
            {
                roots.push(root.clone());
            }
        }
        roots
    } else {
        route.roots.clone()
    };
    if write_roots.is_empty() {
        return Err(AppError::InternalServerError(
            "index object write failed: no reachable route roots".into(),
        ));
    }
    let owner_root = write_roots[0].clone();
    let owner_version_path = object_version_path_for_state(
        &owner_root,
        &metadata.fragment,
        partition_id,
        &metadata.object_key,
        &metadata.version_id,
        trashed,
    );
    write_atomic(&owner_version_path, &version_bytes).await?;
    if let Some(bytes) = &latest_bytes {
        let owner_latest_path = latest_state_path_for_state(
            &owner_root,
            &fragment,
            partition_id,
            &metadata.object_key,
            trashed,
        );
        write_atomic(&owner_latest_path, bytes).await?;
    }
    append_wal(&owner_root, &fragment, partition_id, &wal)
        .await
        .map(|_| ())?;
    if write_roots.len() > 1 {
        let replica_roots = write_roots[1..].to_vec();
        let metadata_fragment = metadata.fragment.clone();
        let object_key = metadata.object_key.clone();
        let version_id = metadata.version_id.clone();
        let version_payload = version_bytes.clone();
        let latest_payload = latest_bytes.clone();
        let wal_clone = wal.clone();
        let fragment_clone = fragment.clone();
        let replicate_fut = async move {
            for root in replica_roots {
                let version_path = object_version_path_for_state(
                    &root,
                    &metadata_fragment,
                    partition_id,
                    &object_key,
                    &version_id,
                    trashed,
                );
                write_atomic(&version_path, &version_payload).await?;
                if let Some(bytes) = &latest_payload {
                    let latest_path = latest_state_path_for_state(
                        &root,
                        &fragment_clone,
                        partition_id,
                        &object_key,
                        trashed,
                    );
                    write_atomic(&latest_path, bytes).await?;
                }
                append_wal(&root, &fragment_clone, partition_id, &wal_clone)
                    .await
                    .map(|_| ())?;
            }
            Ok(())
        };
        match replica_write_mode {
            IndexReplicaWriteMode::BackgroundBestEffort => {
                maybe_replicate("index object replica sync", replicate_fut).await;
            }
            IndexReplicaWriteMode::SyncBestEffort => {
                if let Err(e) = replicate_fut.await {
                    warn!("index object replica sync failed: {}", e);
                }
            }
        }
    }
    Ok(())
}

async fn delete_object_version_internal(
    fragment: &str,
    object_key: &str,
    version_id: &str,
    trashed: bool,
) -> Result<bool, AppError> {
    let route = object_partition_route(fragment, object_key)?;
    let partition_id = route.partition_id;
    let owner_root = route.roots[0].clone();
    let owner_path = object_version_path_for_state(
        &owner_root,
        fragment,
        partition_id,
        object_key,
        version_id,
        trashed,
    );
    let mut removed_any = false;
    if fs::try_exists(&owner_path).await.unwrap_or(false) {
        match fs::remove_file(&owner_path).await {
            Ok(()) => {
                if let Some(parent) = owner_path.parent() {
                    let _ = sync_dir(parent).await;
                }
                removed_any = true;
            }
            Err(e) => warn!("index remove failed {}: {}", owner_path.display(), e),
        }
    }

    if !removed_any {
        return Ok(false);
    }

    let next_latest = collect_object_versions_for_state(&route, fragment, object_key, trashed)
        .await
        .into_iter()
        .filter(|metadata| metadata.version_id != version_id)
        .max_by(|left, right| {
            compare_version_recency(
                left.created_at,
                &left.version_id,
                right.created_at,
                &right.version_id,
            )
        })
        .map(|metadata| IndexLatestState {
            version_id: metadata.version_id.clone(),
            metadata,
        });
    let owner_only_route = IndexRoute {
        partition_id,
        roots: vec![owner_root.clone()],
    };
    persist_latest_state_for_state(
        &owner_only_route,
        fragment,
        object_key,
        next_latest.as_ref(),
        trashed,
    )
    .await?;

    let wal = IndexWalRecord::VersionRemoved {
        fragment: fragment.to_string(),
        object_key: object_key.to_string(),
        version_id: version_id.to_string(),
        removed_at: Utc::now().timestamp(),
        from_trash: trashed,
        next_latest: next_latest.clone(),
    };
    let _ = append_wal(&owner_root, fragment, partition_id, &wal).await;
    if route.roots.len() > 1 {
        let replica_roots = route.roots[1..].to_vec();
        let fragment_clone = fragment.to_string();
        let object_key_clone = object_key.to_string();
        let version_id_clone = version_id.to_string();
        let next_latest_clone = next_latest.clone();
        let wal_clone = wal.clone();
        maybe_replicate("index object remove replica sync", async move {
            for root in replica_roots {
                let path = object_version_path_for_state(
                    &root,
                    &fragment_clone,
                    partition_id,
                    &object_key_clone,
                    &version_id_clone,
                    trashed,
                );
                if fs::try_exists(&path).await.unwrap_or(false) {
                    fs::remove_file(&path)
                        .await
                        .map_err(|e| AppError::Io(e, format!("remove {}", path.display())))?;
                    if let Some(parent) = path.parent() {
                        sync_dir(parent).await?;
                    }
                }
                let route = IndexRoute {
                    partition_id,
                    roots: vec![root.clone()],
                };
                persist_latest_state_for_state(
                    &route,
                    &fragment_clone,
                    &object_key_clone,
                    next_latest_clone.as_ref(),
                    trashed,
                )
                .await?;
                append_wal(&root, &fragment_clone, partition_id, &wal_clone)
                    .await
                    .map(|_| ())?;
            }
            Ok(())
        })
        .await;
    }

    Ok(true)
}

async fn clear_fragment_trash_partitions_inner<I>(
    fragment: &str,
    partition_ids: I,
) -> Result<usize, AppError>
where
    I: IntoIterator<Item = u16>,
{
    let mut touched_partitions = 0usize;
    let cleared_at = Utc::now().timestamp();

    for partition_id in partition_ids {
        let route = route_for_partition_id(partition_id)?;
        let mut touched = false;

        for root in &route.roots {
            let partition_root = partition_dir(root, fragment, partition_id);
            if !fs::try_exists(&partition_root).await.unwrap_or(false) {
                continue;
            }

            let trash_root = partition_root.join(INDEX_TRASH_DIR);
            let checkpoint_path = latest_checkpoint_path(root, fragment, partition_id);
            let existing_checkpoint =
                read_messagepack_if_exists::<IndexCheckpoint>(&checkpoint_path).await?;
            let runtime = INDEX_PARTITION_RUNTIME
                .lock()
                .await
                .get(&runtime_key(root, fragment, partition_id))
                .cloned();
            let (needs_checkpoint_refresh, current_sequence, live_latest, sessions) =
                if let Some(runtime) = runtime {
                    let mut runtime = runtime.lock().await;
                    let live_latest = runtime.materialized.live_latest.values().cloned().collect();
                    let sessions = runtime.materialized.sessions.values().cloned().collect();
                    let needs = !runtime.materialized.trash_latest.is_empty();
                    runtime.materialized.trash_latest.clear();
                    (
                        needs,
                        runtime.manifest.next_sequence.saturating_sub(1),
                        live_latest,
                        sessions,
                    )
                } else {
                    let needs = existing_checkpoint
                        .as_ref()
                        .map(|checkpoint| !checkpoint.trash_latest.is_empty())
                        .unwrap_or(false);
                    let current_sequence = load_wal_manifest(root, fragment, partition_id)
                        .await?
                        .next_sequence
                        .saturating_sub(1);
                    let (live_latest, sessions) = match existing_checkpoint.as_ref() {
                        Some(checkpoint) if checkpoint.up_to_sequence >= current_sequence => {
                            (checkpoint.live_latest.clone(), checkpoint.sessions.clone())
                        }
                        _ => (
                            collect_partition_latest_states_on_root(
                                root,
                                fragment,
                                partition_id,
                                false,
                            )
                            .await?,
                            materialize_partition_sessions_on_root(root, fragment, partition_id)
                                .await?,
                        ),
                    };
                    (needs, current_sequence, live_latest, sessions)
                };

            if fs::try_exists(&trash_root).await.unwrap_or(false) || needs_checkpoint_refresh {
                let checkpoint = IndexCheckpoint {
                    created_at: cleared_at,
                    up_to_sequence: current_sequence,
                    live_latest,
                    trash_latest: Vec::new(),
                    sessions,
                };
                let checkpoint_bytes = rmp_serde::to_vec(&checkpoint).map_err(|e| {
                    AppError::InternalServerError(format!(
                        "serialize trash-cleared checkpoint: {}",
                        e
                    ))
                })?;
                write_atomic(&checkpoint_path, &checkpoint_bytes).await?;
                touched = true;
            }

            if fs::try_exists(&trash_root).await.unwrap_or(false) {
                fs::remove_dir_all(&trash_root)
                    .await
                    .map_err(|e| AppError::Io(e, format!("remove {}", trash_root.display())))?;
                let _ = sync_dir(&partition_root).await;
                touched = true;
            }
        }

        if touched {
            touched_partitions += 1;
        }
    }

    Ok(touched_partitions)
}

pub async fn clear_fragment_trash_in_partitions(
    fragment: &str,
    touched_partitions: &BTreeSet<u16>,
) -> Result<usize, AppError> {
    clear_fragment_trash_partitions_inner(fragment, touched_partitions.iter().copied()).await
}

async fn read_object_version_for_state(
    fragment: &str,
    object_key: &str,
    version_id: &str,
    trashed: bool,
) -> Result<ObjectMetadata, AppError> {
    let route = object_partition_route(fragment, object_key)?;
    let mut replicas = Vec::<(PathBuf, SystemTime)>::new();
    for root in &route.roots {
        let path = object_version_path_for_state(
            root,
            fragment,
            route.partition_id,
            object_key,
            version_id,
            trashed,
        );
        if !fs::try_exists(&path).await.unwrap_or(false) {
            continue;
        }
        let modified = fs::metadata(&path)
            .await
            .ok()
            .and_then(|meta| meta.modified().ok())
            .unwrap_or(SystemTime::UNIX_EPOCH);
        replicas.push((path, modified));
    }
    replicas.sort_by(|left, right| right.1.cmp(&left.1));
    let mut best: Option<(ObjectMetadata, SystemTime)> = None;
    for (path, modified) in replicas {
        match read_messagepack::<ObjectMetadata>(&path).await {
            Ok(metadata) => match &best {
                None => best = Some((metadata, modified)),
                Some((current, current_modified)) => {
                    if metadata_is_newer(&metadata, current)
                        || (metadata.created_at == current.created_at
                            && metadata.version_id == current.version_id
                            && modified > *current_modified)
                    {
                        best = Some((metadata, modified));
                    }
                }
            },
            Err(e) => warn!("index version read failed {}: {}", path.display(), e),
        }
    }
    best.map(|(metadata, _)| metadata).ok_or(AppError::NotFound)
}

fn absorb_latest_state(
    latest_by_key: &mut HashMap<String, IndexLatestState>,
    candidate: IndexLatestState,
) {
    match latest_by_key.get(&candidate.metadata.object_key) {
        None => {
            latest_by_key.insert(candidate.metadata.object_key.clone(), candidate);
        }
        Some(current) => {
            if metadata_is_newer(&candidate.metadata, &current.metadata) {
                latest_by_key.insert(candidate.metadata.object_key.clone(), candidate);
            }
        }
    }
}

async fn scan_fragment_latest_states_filtered(
    fragment: &str,
    trashed: bool,
    prefix: Option<&str>,
) -> Result<Vec<IndexLatestState>, AppError> {
    let mut latest_by_key = HashMap::<String, IndexLatestState>::new();
    let prefix = prefix.map(canonical_prefix);

    for partition_id in 0..INDEX_PARTITION_COUNT {
        let route = route_for_partition_id(partition_id)?;
        for root in &route.roots {
            let partition_root = partition_dir(root, fragment, partition_id);
            if !fs::try_exists(&partition_root).await.unwrap_or(false) {
                continue;
            }
            if let Some(checkpoint) = read_current_checkpoint(&root, fragment, partition_id).await?
            {
                for state in if trashed {
                    checkpoint.trash_latest
                } else {
                    checkpoint.live_latest
                } {
                    if prefix
                        .as_deref()
                        .map(|pfx| object_key_matches_prefix(&state.metadata.object_key, pfx))
                        .unwrap_or(true)
                    {
                        absorb_latest_state(&mut latest_by_key, state);
                    }
                }
                continue;
            }

            for state in
                collect_partition_latest_states_on_root(&root, fragment, partition_id, trashed)
                    .await?
            {
                if prefix
                    .as_deref()
                    .map(|pfx| object_key_matches_prefix(&state.metadata.object_key, pfx))
                    .unwrap_or(true)
                {
                    absorb_latest_state(&mut latest_by_key, state);
                }
            }
        }
    }

    Ok(latest_by_key.into_values().collect())
}

async fn scan_fragment_latest_states(
    fragment: &str,
    trashed: bool,
) -> Result<Vec<IndexLatestState>, AppError> {
    scan_fragment_latest_states_filtered(fragment, trashed, None).await
}

fn partition_versions_root(root: &Path, fragment: &str, partition_id: u16, trashed: bool) -> PathBuf {
    partition_dir(root, fragment, partition_id).join(if trashed {
        INDEX_TRASH_DIR
    } else {
        INDEX_OBJECTS_DIR
    })
}

async fn emit_partition_versions_on_partition(
    tx: &mpsc::Sender<Result<ObjectMetadata, AppError>>,
    fragment: &str,
    partition_id: u16,
    trashed: bool,
) -> Result<(), AppError> {
    let route = route_for_partition_id(partition_id)?;
    let mut seen = HashSet::<PathBuf>::new();

    for root in &route.roots {
        let partition_root = partition_dir(root, fragment, partition_id);
        if !fs::try_exists(&partition_root).await.unwrap_or(false) {
            continue;
        }

        let objects_root = partition_versions_root(root, fragment, partition_id, trashed);
        if !fs::try_exists(&objects_root).await.unwrap_or(false) {
            continue;
        }

        let mut bucket_dirs = match fs::read_dir(&objects_root).await {
            Ok(entries) => entries,
            Err(e) => {
                return Err(AppError::Io(
                    e,
                    format!("read partition version root {}", objects_root.display()),
                ));
            }
        };

        while let Ok(Some(bucket_dir)) = bucket_dirs.next_entry().await {
            let mut object_dirs = match fs::read_dir(bucket_dir.path()).await {
                Ok(entries) => entries,
                Err(_) => continue,
            };
            while let Ok(Some(object_dir)) = object_dirs.next_entry().await {
                let mut files = match fs::read_dir(object_dir.path()).await {
                    Ok(entries) => entries,
                    Err(_) => continue,
                };
                while let Ok(Some(file)) = files.next_entry().await {
                    let name = file.file_name().to_string_lossy().to_string();
                    if !is_version_entry(&name) {
                        continue;
                    }
                    let Ok(relative_path) = file.path().strip_prefix(&objects_root).map(Path::to_path_buf) else {
                        continue;
                    };
                    if !seen.insert(relative_path) {
                        continue;
                    }
                    match read_messagepack::<ObjectMetadata>(&file.path()).await {
                        Ok(metadata) => {
                            if tx.send(Ok(metadata)).await.is_err() {
                                return Ok(());
                            }
                        }
                        Err(e) => warn!(
                            "index version scan decode failed {}: {}",
                            file.path().display(),
                            e
                        ),
                    }
                }
            }
        }
    }

    Ok(())
}

async fn run_fragment_versions_stream(
    fragment: String,
    trashed: bool,
    tx: mpsc::Sender<Result<ObjectMetadata, AppError>>,
) -> Result<(), AppError> {
    for partition_id in 0..INDEX_PARTITION_COUNT {
        emit_partition_versions_on_partition(&tx, &fragment, partition_id, trashed).await?;
        if tx.is_closed() {
            break;
        }
    }
    Ok(())
}

pub fn stream_fragment_object_versions(
    fragment: String,
    trashed: bool,
) -> mpsc::Receiver<Result<ObjectMetadata, AppError>> {
    let (tx, rx) = mpsc::channel(INDEX_FRAGMENT_VERSION_STREAM_BUFFER);
    tokio::spawn(async move {
        if let Err(err) = run_fragment_versions_stream(fragment, trashed, tx.clone()).await {
            let _ = tx.send(Err(err)).await;
        }
    });
    rx
}

pub async fn open_session(
    manifest: &UploadSessionManifest,
    reservation: Option<ReservationReceipt>,
) -> Result<(), AppError> {
    let session_guard = session_lock(&manifest.fragment, &manifest.version_id).await;
    let _session_guard = session_guard.lock().await;
    let route = session_partition_route(&manifest.fragment, &manifest.version_id)?;
    let state = IndexSessionState {
        manifest: manifest.clone(),
        reservation,
        parts: BTreeMap::new(),
        last_activity_at: manifest.created_at,
        finalized_at: None,
        aborted_at: None,
    };
    let fragment = manifest.fragment.clone();
    let wal = IndexWalRecord::SessionOpened {
        session: state.clone(),
    };
    let owner_root = route.roots[0].clone();
    append_wal(&owner_root, &fragment, route.partition_id, &wal)
        .await
        .map(|_| ())?;
    cache_active_session(&state).await;
    persist_session_snapshot_on_roots(
        &route,
        &fragment,
        &manifest.version_id,
        &state,
        IndexReplicaWriteMode::BackgroundBestEffort,
    )
    .await?;
    if route.roots.len() > 1 {
        let replica_roots = route.roots[1..].to_vec();
        let fragment_clone = fragment.clone();
        let wal_clone = wal.clone();
        maybe_replicate("index session open replica sync", async move {
            for root in replica_roots {
                append_wal(&root, &fragment_clone, route.partition_id, &wal_clone)
                    .await
                    .map(|_| ())?;
            }
            Ok(())
        })
        .await;
    }
    Ok(())
}

async fn load_session_uncached(
    fragment: &str,
    version_id: &str,
) -> Result<Option<IndexSessionState>, AppError> {
    let route = session_partition_route(fragment, version_id)?;
    let mut best: Option<IndexSessionState> = None;
    for root in &route.roots {
        let runtime_key = runtime_key(root, fragment, route.partition_id);
        let mut candidate = if let Some(runtime) = INDEX_PARTITION_RUNTIME
            .lock()
            .await
            .get(&runtime_key)
            .cloned()
        {
            let runtime = runtime.lock().await;
            runtime.materialized.sessions.get(version_id).cloned()
        } else {
            None
        };

        if candidate.is_none() {
            match materialize_partition_sessions_on_root(root, fragment, route.partition_id).await {
                Ok(states) => {
                    candidate = states
                        .into_iter()
                        .find(|state| state.manifest.version_id == version_id);
                }
                Err(e) => {
                    warn!(
                        "index session materialize failed fragment={} version={} root={}: {}",
                        fragment,
                        version_id,
                        root.display(),
                        e
                    );
                }
            }
        }

        if let Some(mut state) = candidate {
            if let Err(e) =
                enrich_session_state_from_root(root, fragment, route.partition_id, &mut state).await
            {
                warn!(
                    "index session part materialize failed fragment={} version={} root={}: {}",
                    fragment,
                    version_id,
                    root.display(),
                    e
                );
            }
            let replace = best
                .as_ref()
                .map(|current| {
                    state.parts.len() > current.parts.len()
                        || (state.parts.len() == current.parts.len()
                            && state.last_activity_at > current.last_activity_at)
                })
                .unwrap_or(true);
            if replace {
                best = Some(normalize_session_state(state));
            }
        }
    }
    Ok(best)
}

pub async fn load_session(
    fragment: &str,
    version_id: &str,
) -> Result<Option<IndexSessionState>, AppError> {
    Ok(load_session_uncached(fragment, version_id)
        .await?
        .map(normalize_session_state))
}

#[cfg(test)]
pub async fn clear_runtime_caches_for_tests() {
    INDEX_ACTIVE_SESSIONS.lock().await.clear();
    INDEX_ACTIVE_SESSION_FRAGMENTS_SCANNED.lock().await.clear();
    INDEX_PARTITION_RUNTIME.lock().await.clear();
    INDEX_WAL_MANIFEST_CACHE.lock().await.clear();
}

pub async fn remove_session(
    fragment: &str,
    version_id: &str,
    reason: &str,
) -> Result<(), AppError> {
    let session_guard = session_lock(fragment, version_id).await;
    let _session_guard = session_guard.lock().await;
    let route = session_partition_route(fragment, version_id)?;
    let session_state = load_session_uncached(fragment, version_id).await?;
    let existed = session_state.is_some();
    let wal = IndexWalRecord::SessionRemoved {
        fragment: fragment.to_string(),
        version_id: version_id.to_string(),
        removed_at: Utc::now().timestamp(),
        reason: reason.to_string(),
    };
    if let Some(state) = session_state.as_ref().filter(|state| {
        !state.parts.is_empty() || !state.manifest.planned_parts.is_empty()
    }) {
        let receipt = RemovedSessionReceipt {
            state: state.clone(),
            removed_at: match &wal {
                IndexWalRecord::SessionRemoved { removed_at, .. } => *removed_at,
                _ => 0,
            },
            reason: reason.to_string(),
        };
        persist_removed_session_receipt_on_roots(
            &route,
            fragment,
            version_id,
            &receipt,
            IndexReplicaWriteMode::BackgroundBestEffort,
        )
        .await?;
    }
    let owner_root = route.roots[0].clone();
    let owner_path = session_state_path(&owner_root, fragment, route.partition_id, version_id);
    if fs::try_exists(&owner_path).await.unwrap_or(false) {
        if fs::remove_file(&owner_path).await.is_ok() {
            if let Some(parent) = owner_path.parent() {
                let _ = sync_dir(parent).await;
            }
        }
    }
    let _ = append_wal(&owner_root, fragment, route.partition_id, &wal).await;
    if route.roots.len() > 1 {
        let replica_roots = route.roots[1..].to_vec();
        let fragment_clone = fragment.to_string();
        let version_clone = version_id.to_string();
        let wal_clone = wal.clone();
        maybe_replicate("index session remove replica sync", async move {
            for root in replica_roots {
                let path =
                    session_state_path(&root, &fragment_clone, route.partition_id, &version_clone);
                if fs::try_exists(&path).await.unwrap_or(false) {
                    fs::remove_file(&path)
                        .await
                        .map_err(|e| AppError::Io(e, format!("remove {}", path.display())))?;
                    if let Some(parent) = path.parent() {
                        sync_dir(parent).await?;
                    }
                }
                append_wal(&root, &fragment_clone, route.partition_id, &wal_clone)
                    .await
                    .map(|_| ())?;
            }
            Ok(())
        })
        .await;
    }
    remove_active_session(fragment, version_id).await;
    if !existed {
        return Err(AppError::NotFound);
    }
    Ok(())
}

pub async fn list_active_sessions(fragment: &str) -> Result<Vec<IndexSessionState>, AppError> {
    if INDEX_ACTIVE_SESSION_FRAGMENTS_SCANNED
        .lock()
        .await
        .contains(fragment)
    {
        let cached = INDEX_ACTIVE_SESSIONS
            .lock()
            .await
            .get(fragment)
            .cloned()
            .unwrap_or_default();
        return Ok(cached.into_values().collect());
    }

    let mut sessions = HashMap::<String, IndexSessionState>::new();
    for partition_id in 0..INDEX_PARTITION_COUNT {
        let route = route_for_partition_id(partition_id)?;
        for root in &route.roots {
            let partition_root = partition_dir(root, fragment, partition_id);
            if !fs::try_exists(&partition_root).await.unwrap_or(false) {
                continue;
            }
            let partition_sessions =
                materialize_partition_sessions_on_root(root, fragment, partition_id).await?;
            for state in partition_sessions {
                if state.finalized_at.is_some() || state.aborted_at.is_some() {
                    continue;
                }
                sessions
                    .entry(state.manifest.version_id.clone())
                    .or_insert_with(|| normalize_session_state(state));
            }
            break;
        }
    }

    INDEX_ACTIVE_SESSIONS
        .lock()
        .await
        .insert(fragment.to_string(), sessions.clone());
    INDEX_ACTIVE_SESSION_FRAGMENTS_SCANNED
        .lock()
        .await
        .insert(fragment.to_string());

    Ok(sessions.into_values().collect())
}

pub async fn collect_runtime_upload_stats() -> UploadRuntimeStats {
    let now = Utc::now().timestamp();
    let guard = INDEX_ACTIVE_SESSIONS.lock().await;
    let mut stats = UploadRuntimeStats {
        active_fragments: guard.len() as u64,
        ..UploadRuntimeStats::default()
    };
    for sessions in guard.values() {
        for session in sessions.values() {
            stats.active_sessions += 1;
            stats.reserved_bytes = stats
                .reserved_bytes
                .saturating_add(session.reservation.as_ref().map(|r| r.amount).unwrap_or(0));
            stats.committed_parts = stats
                .committed_parts
                .saturating_add(session.parts.len() as u64);
            stats.committed_bytes = stats.committed_bytes.saturating_add(
                session
                    .parts
                    .values()
                    .map(|part| part.size_bytes)
                    .sum::<u64>(),
            );
            let age_seconds = now.saturating_sub(session.manifest.created_at).max(0) as u64;
            stats.oldest_session_age_seconds = Some(
                stats
                    .oldest_session_age_seconds
                    .map(|current| current.max(age_seconds))
                    .unwrap_or(age_seconds),
            );
        }
    }
    stats
}

pub async fn collect_index_runtime_stats() -> IndexRuntimeStats {
    let prefix_pending = PREFIX_PROJECTION_TASKS
        .lock()
        .await
        .values()
        .filter(|state| state.task.running || state.task.rerun_requested)
        .count() as u64;

    IndexRuntimeStats {
        loaded_partition_runtimes: INDEX_PARTITION_RUNTIME.lock().await.len() as u64,
        wal_appenders: INDEX_WAL_APPENDERS.lock().await.len() as u64,
        prefix_projection_fragments_pending: prefix_pending,
    }
}

pub async fn count_fragment_latest_objects(fragment: &str, trashed: bool) -> Result<u64, AppError> {
    Ok(scan_fragment_latest_states(fragment, trashed).await?.len() as u64)
}

pub fn get_session_state_paths(fragment: &str, version_id: &str) -> Result<Vec<PathBuf>, AppError> {
    let route = session_partition_route(fragment, version_id)?;
    Ok(route
        .roots
        .iter()
        .map(|root| session_state_path(root, fragment, route.partition_id, version_id))
        .collect())
}

pub fn get_removed_session_receipt_paths(
    fragment: &str,
    version_id: &str,
) -> Result<Vec<PathBuf>, AppError> {
    let route = session_partition_route(fragment, version_id)?;
    Ok(route
        .roots
        .iter()
        .map(|root| session_removed_receipt_path(root, fragment, route.partition_id, version_id))
        .collect())
}

pub async fn read_removed_session_receipt(
    fragment: &str,
    version_id: &str,
) -> Result<Option<RemovedSessionReceipt>, AppError> {
    let mut best: Option<(RemovedSessionReceipt, SystemTime)> = None;
    for path in get_removed_session_receipt_paths(fragment, version_id)? {
        let modified = match fs::metadata(&path).await {
            Ok(meta) => meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            Err(e)
                if e.kind() == std::io::ErrorKind::NotFound
                    || e.kind() == std::io::ErrorKind::NotADirectory =>
            {
                continue;
            }
            Err(e) => return Err(AppError::Io(e, format!("metadata {}", path.display()))),
        };
        let Some(receipt) = read_messagepack_if_exists::<RemovedSessionReceipt>(&path).await? else {
            continue;
        };
        match &best {
            None => best = Some((receipt, modified)),
            Some((current, current_modified)) => {
                if receipt.removed_at > current.removed_at
                    || (receipt.removed_at == current.removed_at && modified > *current_modified)
                {
                    best = Some((receipt, modified));
                }
            }
        }
    }
    Ok(best.map(|(receipt, _)| receipt))
}

pub async fn delete_removed_session_receipt(
    fragment: &str,
    version_id: &str,
) -> Result<(), AppError> {
    for path in get_removed_session_receipt_paths(fragment, version_id)? {
        match fs::remove_file(&path).await {
            Ok(()) => {
                if let Some(parent) = path.parent() {
                    let _ = sync_dir(parent).await;
                }
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::NotFound
                    || e.kind() == std::io::ErrorKind::NotADirectory => {}
            Err(e) => return Err(AppError::Io(e, format!("remove {}", path.display()))),
        }
    }
    Ok(())
}

pub async fn publish_object(metadata: &ObjectMetadata) -> Result<(), AppError> {
    write_object_version_internal(
        metadata,
        false,
        false,
        IndexReplicaWriteMode::BackgroundBestEffort,
        false,
    )
    .await?;
    schedule_prefix_projection_for_key(metadata.fragment.clone(), metadata.object_key.clone());
    Ok(())
}

pub async fn upsert_object_version(metadata: &ObjectMetadata) -> Result<(), AppError> {
    upsert_object_version_with_replica_mode(
        metadata,
        IndexReplicaWriteMode::BackgroundBestEffort,
    )
    .await
}

pub async fn upsert_object_version_with_replica_mode(
    metadata: &ObjectMetadata,
    replica_write_mode: IndexReplicaWriteMode,
) -> Result<(), AppError> {
    write_object_version_internal(metadata, false, false, replica_write_mode, false).await?;
    schedule_prefix_projection_for_key(metadata.fragment.clone(), metadata.object_key.clone());
    Ok(())
}

pub async fn upsert_object_version_on_available_roots_with_replica_mode(
    metadata: &ObjectMetadata,
    replica_write_mode: IndexReplicaWriteMode,
) -> Result<(), AppError> {
    write_object_version_internal(metadata, false, false, replica_write_mode, true).await?;
    schedule_prefix_projection_for_key(metadata.fragment.clone(), metadata.object_key.clone());
    Ok(())
}

pub async fn upsert_trashed_object_version(metadata: &ObjectMetadata) -> Result<(), AppError> {
    upsert_trashed_object_version_with_replica_mode(
        metadata,
        IndexReplicaWriteMode::BackgroundBestEffort,
    )
    .await
}

pub async fn upsert_trashed_object_version_with_replica_mode(
    metadata: &ObjectMetadata,
    replica_write_mode: IndexReplicaWriteMode,
) -> Result<(), AppError> {
    write_object_version_internal(metadata, false, true, replica_write_mode, false).await?;
    schedule_prefix_projection_for_key(metadata.fragment.clone(), metadata.object_key.clone());
    Ok(())
}

pub async fn upsert_trashed_object_version_on_available_roots_with_replica_mode(
    metadata: &ObjectMetadata,
    replica_write_mode: IndexReplicaWriteMode,
) -> Result<(), AppError> {
    write_object_version_internal(metadata, false, true, replica_write_mode, true).await?;
    schedule_prefix_projection_for_key(metadata.fragment.clone(), metadata.object_key.clone());
    Ok(())
}

pub async fn list_live_object_versions_metadata(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let route = object_partition_route(fragment, object_key)?;
    let mut versions = collect_object_versions(&route, fragment, object_key).await;
    versions.sort_by(|left, right| {
        compare_version_recency(
            right.created_at,
            &right.version_id,
            left.created_at,
            &left.version_id,
        )
    });
    Ok(versions)
}

pub async fn list_trashed_object_versions_metadata(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let route = object_partition_route(fragment, object_key)?;
    let mut versions = collect_object_versions_for_state(&route, fragment, object_key, true).await;
    versions.sort_by(|left, right| {
        compare_version_recency(
            right.created_at,
            &right.version_id,
            left.created_at,
            &left.version_id,
        )
    });
    Ok(versions)
}

pub async fn list_fragment_objects(fragment: &str) -> Result<Vec<ObjectMetadata>, AppError> {
    let mut objects = scan_fragment_latest_states(fragment, false)
        .await?
        .into_iter()
        .map(|state| state.metadata)
        .collect::<Vec<_>>();
    objects.sort_by(|left, right| {
        left.object_key
            .cmp(&right.object_key)
            .then(left.version_id.cmp(&right.version_id))
    });
    Ok(objects)
}

pub async fn list_fragment_objects_for_prefix(
    fragment: &str,
    prefix: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let mut objects = scan_fragment_latest_states_filtered(fragment, false, Some(prefix))
        .await?
        .into_iter()
        .map(|state| state.metadata)
        .collect::<Vec<_>>();
    objects.sort_by(|left, right| {
        left.object_key
            .cmp(&right.object_key)
            .then(left.version_id.cmp(&right.version_id))
    });
    Ok(objects)
}

pub async fn list_fragment_trash_objects(fragment: &str) -> Result<Vec<ObjectMetadata>, AppError> {
    let mut objects = scan_fragment_latest_states(fragment, true)
        .await?
        .into_iter()
        .map(|state| state.metadata)
        .collect::<Vec<_>>();
    objects.sort_by(|left, right| {
        left.object_key
            .cmp(&right.object_key)
            .then(left.version_id.cmp(&right.version_id))
    });
    Ok(objects)
}

pub async fn list_fragment_trash_objects_for_prefix(
    fragment: &str,
    prefix: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let mut objects = scan_fragment_latest_states_filtered(fragment, true, Some(prefix))
        .await?
        .into_iter()
        .map(|state| state.metadata)
        .collect::<Vec<_>>();
    objects.sort_by(|left, right| {
        left.object_key
            .cmp(&right.object_key)
            .then(left.version_id.cmp(&right.version_id))
    });
    Ok(objects)
}

pub async fn delete_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<bool, AppError> {
    let removed = delete_object_version_internal(fragment, object_key, version_id, false).await?;
    if removed {
        schedule_prefix_projection_for_key(fragment.to_string(), object_key.to_string());
    }
    Ok(removed)
}

pub async fn delete_trashed_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<bool, AppError> {
    let removed = delete_object_version_internal(fragment, object_key, version_id, true).await?;
    if removed {
        schedule_prefix_projection_for_key(fragment.to_string(), object_key.to_string());
    }
    Ok(removed)
}

pub async fn move_object_version_to_trash(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let metadata = match read_object_version(fragment, object_key, version_id).await {
        Ok(metadata) => metadata,
        Err(AppError::NotFound) => return Ok(None),
        Err(e) => return Err(e),
    };

    write_object_version_internal(
        &metadata,
        true,
        true,
        IndexReplicaWriteMode::BackgroundBestEffort,
        false,
    )
    .await?;
    delete_object_version_internal(fragment, object_key, version_id, false).await?;
    schedule_prefix_projection_for_key(fragment.to_string(), object_key.to_string());
    Ok(Some(metadata))
}

pub async fn restore_object_version_from_trash(
    fragment: &str,
    object_key: &str,
    version_id: &str,
    force_latest: bool,
) -> Result<Option<ObjectMetadata>, AppError> {
    let metadata = match read_object_version_for_state(fragment, object_key, version_id, true).await
    {
        Ok(metadata) => metadata,
        Err(AppError::NotFound) => return Ok(None),
        Err(e) => return Err(e),
    };

    write_object_version_internal(
        &metadata,
        force_latest,
        false,
        IndexReplicaWriteMode::BackgroundBestEffort,
        false,
    )
    .await?;
    delete_object_version_internal(fragment, object_key, version_id, true).await?;
    schedule_prefix_projection_for_key(fragment.to_string(), object_key.to_string());
    Ok(Some(metadata))
}

pub async fn read_latest_object(
    fragment: &str,
    object_key: &str,
) -> Result<ObjectMetadata, AppError> {
    let route = object_partition_route(fragment, object_key)?;
    let latest = load_latest_state(&route, fragment, object_key)
        .await
        .ok_or(AppError::NotFound)?;

    // Latest markers are an acceleration structure. Rehydrate the named version
    // before serving so stale cached latest payload cannot hide repaired or newer
    // authoritative version metadata.
    match read_object_version_for_state(fragment, object_key, &latest.version_id, false).await {
        Ok(metadata) => Ok(metadata),
        Err(AppError::NotFound) => collect_object_versions(&route, fragment, object_key)
            .await
            .into_iter()
            .max_by(|left, right| {
                compare_version_recency(
                    left.created_at,
                    &left.version_id,
                    right.created_at,
                    &right.version_id,
                )
            })
            .ok_or(AppError::NotFound),
        Err(e) => Err(e),
    }
}

pub async fn read_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<ObjectMetadata, AppError> {
    read_object_version_for_state(fragment, object_key, version_id, false).await
}

pub async fn read_trashed_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<ObjectMetadata, AppError> {
    read_object_version_for_state(fragment, object_key, version_id, true).await
}

pub fn get_object_version_paths(
    fragment: &str,
    object_key: &str,
    version_id: &str,
    trashed: bool,
) -> Result<Vec<PathBuf>, AppError> {
    let route = object_partition_route(fragment, object_key)?;
    Ok(route
        .roots
        .iter()
        .map(|root| {
            object_version_path_for_state(
                root,
                fragment,
                route.partition_id,
                object_key,
                version_id,
                trashed,
            )
        })
        .collect())
}
