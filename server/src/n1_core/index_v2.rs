use crate::enums::app_error_enum::AppError;
use crate::n1_core::constants::{PREFIX_DELIMITER_SEGMENT_PREFIX, PREFIX_SEGMENT_EXT};
use crate::n1_core::disk_manager::{
    ObjectMetadata, ObjectPathManager, PartMetadata, PrefixIndexEntry, PrefixIndexSegment,
    ReservationReceipt, compare_version_recency, version_recency_is_newer,
};
use crate::n1_core::prefix_delimiter::build_delimiter_compacted_from_entries;
use crate::n1_core::upload::UploadSessionManifest;
use chrono::Utc;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::time::SystemTime;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
#[cfg(not(test))]
use tokio::sync::Semaphore;
#[cfg(not(test))]
use tokio::time::{Duration, sleep};
use tracing::warn;
use ulid::Ulid;

const INDEX_V2_DIR: &str = "index_v2";
const INDEX_V2_PARTITIONS_DIR: &str = "partitions";
const INDEX_V2_SESSIONS_DIR: &str = "sessions";
const INDEX_V2_OBJECTS_DIR: &str = "objects";
const INDEX_V2_TRASH_DIR: &str = "trash";
const INDEX_V2_WAL_DIR: &str = "wal";
const INDEX_V2_WAL_SEGMENTS_DIR: &str = "segments";
const INDEX_V2_WAL_MANIFEST_FILE: &str = "manifest.msgpack";
const INDEX_V2_LEGACY_ACTIVE_WAL: &str = "active.wal";
const INDEX_V2_CHECKPOINTS_DIR: &str = "checkpoints";
const INDEX_V2_LATEST_CHECKPOINT_FILE: &str = "latest.msgpack";
const INDEX_V2_PREFIX_PROJECTION_CURSOR_FILE: &str = "prefix_projection.cursor.msgpack";
const INDEX_V2_LATEST_SUFFIX: &str = ".latest.msgpack";
const INDEX_V2_SESSION_SUFFIX: &str = ".session.msgpack";
const INDEX_V2_OBJECT_SUFFIX: &str = ".msgpack";
const INDEX_V2_PARTITION_COUNT: u16 = 256;
const INDEX_V2_WAL_SEGMENT_MAX_BYTES: u64 = 16 * 1024;
const INDEX_V2_CHECKPOINT_RECORD_INTERVAL: u64 = 32;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexV2SessionState {
    pub manifest: UploadSessionManifest,
    #[serde(default)]
    pub reservation: Option<ReservationReceipt>,
    #[serde(default)]
    pub parts: BTreeMap<u64, PartMetadata>,
    #[serde(default)]
    pub finalized_at: Option<i64>,
    #[serde(default)]
    pub aborted_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexV2LatestState {
    pub version_id: String,
    pub metadata: ObjectMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum IndexV2WalRecord {
    SessionOpened {
        session: IndexV2SessionState,
    },
    PartCommitted {
        fragment: String,
        version_id: String,
        part_index: u64,
        part: PartMetadata,
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
    },
    SessionRemoved {
        fragment: String,
        version_id: String,
        removed_at: i64,
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexV2WalEnvelope {
    sequence: u64,
    recorded_at: i64,
    record: IndexV2WalRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexV2WalManifest {
    active_segment_id: u64,
    next_sequence: u64,
    last_checkpoint_sequence: u64,
}

impl Default for IndexV2WalManifest {
    fn default() -> Self {
        Self {
            active_segment_id: 1,
            next_sequence: 1,
            last_checkpoint_sequence: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexV2Checkpoint {
    created_at: i64,
    up_to_sequence: u64,
    #[serde(default)]
    live_latest: Vec<IndexV2LatestState>,
    #[serde(default)]
    trash_latest: Vec<IndexV2LatestState>,
    #[serde(default)]
    sessions: Vec<IndexV2SessionState>,
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
}

static PREFIX_PROJECTION_TASKS: LazyLock<Mutex<HashMap<String, PrefixProjectionTaskState>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(not(test))]
static PREFIX_PROJECTION_IO_GATE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(1));

#[derive(Debug, Clone)]
struct IndexV2Route {
    partition_id: u16,
    roots: Vec<PathBuf>,
}

fn hash_key(parts: &[&str]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    hasher.finalize().into()
}

fn object_partition_route(fragment: &str, object_key: &str) -> Result<IndexV2Route, AppError> {
    route_for_hash(hash_key(&["object", fragment, object_key]))
}

fn session_partition_route(fragment: &str, version_id: &str) -> Result<IndexV2Route, AppError> {
    route_for_hash(hash_key(&["session", fragment, version_id]))
}

fn route_for_hash(hash: [u8; 32]) -> Result<IndexV2Route, AppError> {
    let roots = ObjectPathManager::new().get_all_index_roots();
    if roots.is_empty() {
        return Err(AppError::InternalServerError(
            "No index roots configured".to_string(),
        ));
    }

    let partition_id = (u16::from(hash[0]) << 8 | u16::from(hash[1])) % INDEX_V2_PARTITION_COUNT;
    let owner_idx = usize::from(hash[2]) % roots.len();
    let mut selected = vec![roots[owner_idx].clone()];
    if roots.len() > 1 {
        let replica_offset = 1 + (usize::from(hash[3]) % (roots.len() - 1));
        let replica_idx = (owner_idx + replica_offset) % roots.len();
        if replica_idx != owner_idx {
            selected.push(roots[replica_idx].clone());
        }
    }

    Ok(IndexV2Route {
        partition_id,
        roots: selected,
    })
}

fn partition_dir(root: &Path, fragment: &str, partition_id: u16) -> PathBuf {
    root.join(fragment)
        .join(INDEX_V2_DIR)
        .join(INDEX_V2_PARTITIONS_DIR)
        .join(format!("{:04x}", partition_id))
}

fn session_state_path(root: &Path, fragment: &str, partition_id: u16, version_id: &str) -> PathBuf {
    partition_dir(root, fragment, partition_id)
        .join(INDEX_V2_SESSIONS_DIR)
        .join(format!("{}{}", version_id, INDEX_V2_SESSION_SUFFIX))
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
            INDEX_V2_TRASH_DIR
        } else {
            INDEX_V2_OBJECTS_DIR
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
    object_prefix_dir_for_state(root, fragment, partition_id, object_key, trashed).join(format!(
        "latest{}",
        INDEX_V2_LATEST_SUFFIX
    ))
}

fn object_version_path_for_state(
    root: &Path,
    fragment: &str,
    partition_id: u16,
    object_key: &str,
    version_id: &str,
    trashed: bool,
) -> PathBuf {
    object_prefix_dir_for_state(root, fragment, partition_id, object_key, trashed).join(format!(
        "{}{}",
        version_id, INDEX_V2_OBJECT_SUFFIX
    ))
}

fn wal_dir(root: &Path, fragment: &str, partition_id: u16) -> PathBuf {
    partition_dir(root, fragment, partition_id).join(INDEX_V2_WAL_DIR)
}

fn wal_segments_dir(root: &Path, fragment: &str, partition_id: u16) -> PathBuf {
    wal_dir(root, fragment, partition_id).join(INDEX_V2_WAL_SEGMENTS_DIR)
}

fn legacy_wal_path(root: &Path, fragment: &str, partition_id: u16) -> PathBuf {
    wal_dir(root, fragment, partition_id).join(INDEX_V2_LEGACY_ACTIVE_WAL)
}

fn wal_manifest_path(root: &Path, fragment: &str, partition_id: u16) -> PathBuf {
    wal_dir(root, fragment, partition_id).join(INDEX_V2_WAL_MANIFEST_FILE)
}

fn wal_segment_path(root: &Path, fragment: &str, partition_id: u16, segment_id: u64) -> PathBuf {
    wal_segments_dir(root, fragment, partition_id).join(format!("{:020}.wal", segment_id))
}

fn checkpoints_dir(root: &Path, fragment: &str, partition_id: u16) -> PathBuf {
    partition_dir(root, fragment, partition_id).join(INDEX_V2_CHECKPOINTS_DIR)
}

fn latest_checkpoint_path(root: &Path, fragment: &str, partition_id: u16) -> PathBuf {
    checkpoints_dir(root, fragment, partition_id).join(INDEX_V2_LATEST_CHECKPOINT_FILE)
}

fn prefix_projection_cursor_path(root: &Path, fragment: &str) -> PathBuf {
    root.join(fragment)
        .join(INDEX_V2_DIR)
        .join(INDEX_V2_PREFIX_PROJECTION_CURSOR_FILE)
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

async fn append_length_prefixed_record<T>(path: &Path, record: &T) -> Result<(), AppError>
where
    T: Serialize,
{
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError(format!("No parent dir for {}", path.display()))
    })?;
    fs::create_dir_all(parent)
        .await
        .map_err(|e| AppError::Io(e, format!("mkdir {}", parent.display())))?;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .map_err(|e| AppError::Io(e, format!("open {}", path.display())))?;
    let payload = rmp_serde::to_vec(record)
        .map_err(|e| AppError::InternalServerError(format!("serialize wal record: {}", e)))?;
    let len = u32::try_from(payload.len())
        .map_err(|_| AppError::InternalServerError("WAL record too large".to_string()))?;
    file.write_all(&len.to_le_bytes())
        .await
        .map_err(|e| AppError::Io(e, format!("append len {}", path.display())))?;
    file.write_all(&payload)
        .await
        .map_err(|e| AppError::Io(e, format!("append payload {}", path.display())))?;
    file.sync_all()
        .await
        .map_err(|e| AppError::Io(e, format!("sync wal {}", path.display())))?;
    drop(file);
    sync_dir(parent).await?;
    Ok(())
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

async fn migrate_legacy_wal_if_needed(
    root: &Path,
    fragment: &str,
    partition_id: u16,
) -> Result<(), AppError> {
    let manifest_path = wal_manifest_path(root, fragment, partition_id);
    if fs::try_exists(&manifest_path).await.unwrap_or(false) {
        return Ok(());
    }

    let legacy_path = legacy_wal_path(root, fragment, partition_id);
    if !fs::try_exists(&legacy_path).await.unwrap_or(false) {
        return Ok(());
    }

    let legacy_bytes = fs::read(&legacy_path)
        .await
        .map_err(|e| AppError::Io(e, format!("read {}", legacy_path.display())))?;
    let legacy_records =
        decode_length_prefixed_records::<IndexV2WalRecord>(&legacy_bytes, &legacy_path)?;
    let mut next_sequence = 1u64;
    let migrated_path = wal_segment_path(root, fragment, partition_id, 1);

    if fs::try_exists(&migrated_path).await.unwrap_or(false) {
        fs::remove_file(&migrated_path)
            .await
            .map_err(|e| AppError::Io(e, format!("remove {}", migrated_path.display())))?;
    }

    for record in legacy_records {
        let envelope = IndexV2WalEnvelope {
            sequence: next_sequence,
            recorded_at: Utc::now().timestamp(),
            record,
        };
        append_length_prefixed_record(&migrated_path, &envelope).await?;
        next_sequence += 1;
    }

    fs::remove_file(&legacy_path)
        .await
        .map_err(|e| AppError::Io(e, format!("remove {}", legacy_path.display())))?;
    write_atomic(
        &manifest_path,
        &rmp_serde::to_vec(&IndexV2WalManifest {
            active_segment_id: 1,
            next_sequence,
            last_checkpoint_sequence: 0,
        })
        .map_err(|e| AppError::InternalServerError(format!("serialize wal manifest: {}", e)))?,
    )
    .await?;
    Ok(())
}

async fn load_wal_manifest(
    root: &Path,
    fragment: &str,
    partition_id: u16,
) -> Result<IndexV2WalManifest, AppError> {
    migrate_legacy_wal_if_needed(root, fragment, partition_id).await?;
    Ok(
        read_messagepack_if_exists(&wal_manifest_path(root, fragment, partition_id))
            .await?
            .unwrap_or_default(),
    )
}

async fn persist_wal_manifest(
    root: &Path,
    fragment: &str,
    partition_id: u16,
    manifest: &IndexV2WalManifest,
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
) -> Result<Vec<IndexV2LatestState>, AppError> {
    let mut out = Vec::new();
    let objects_root = partition_dir(root, fragment, partition_id).join(if trashed {
        INDEX_V2_TRASH_DIR
    } else {
        INDEX_V2_OBJECTS_DIR
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
            match read_messagepack::<IndexV2LatestState>(&latest_path).await {
                Ok(state) => out.push(state),
                Err(e) => warn!(
                    "index_v2 checkpoint latest decode failed {}: {}",
                    latest_path.display(),
                    e
                ),
            }
        }
    }

    Ok(out)
}

async fn collect_partition_sessions_on_root(
    root: &Path,
    fragment: &str,
    partition_id: u16,
) -> Result<Vec<IndexV2SessionState>, AppError> {
    let mut out = Vec::new();
    let sessions_dir = partition_dir(root, fragment, partition_id).join(INDEX_V2_SESSIONS_DIR);
    if !fs::try_exists(&sessions_dir).await.unwrap_or(false) {
        return Ok(out);
    }

    let mut entries = match fs::read_dir(&sessions_dir).await {
        Ok(entries) => entries,
        Err(_) => return Ok(out),
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if !entry
            .file_name()
            .to_string_lossy()
            .ends_with(INDEX_V2_SESSION_SUFFIX)
        {
            continue;
        }
        match read_messagepack::<IndexV2SessionState>(&path).await {
            Ok(state) => out.push(state),
            Err(e) => warn!(
                "index_v2 checkpoint session decode failed {}: {}",
                path.display(),
                e
            ),
        }
    }

    Ok(out)
}

async fn maybe_write_checkpoint(
    root: &Path,
    fragment: &str,
    partition_id: u16,
    manifest: &mut IndexV2WalManifest,
) -> Result<(), AppError> {
    let current_sequence = manifest.next_sequence.saturating_sub(1);
    if current_sequence == 0
        || current_sequence.saturating_sub(manifest.last_checkpoint_sequence)
            < INDEX_V2_CHECKPOINT_RECORD_INTERVAL
    {
        return Ok(());
    }

    let live_latest = collect_partition_latest_states_on_root(root, fragment, partition_id, false)
        .await?;
    let trash_latest = collect_partition_latest_states_on_root(root, fragment, partition_id, true)
        .await?;
    let sessions = collect_partition_sessions_on_root(root, fragment, partition_id).await?;
    let checkpoint = IndexV2Checkpoint {
        created_at: Utc::now().timestamp(),
        up_to_sequence: current_sequence,
        live_latest,
        trash_latest,
        sessions,
    };
    let bytes = rmp_serde::to_vec(&checkpoint)
        .map_err(|e| AppError::InternalServerError(format!("serialize checkpoint: {}", e)))?;
    write_atomic(&latest_checkpoint_path(root, fragment, partition_id), &bytes).await?;
    manifest.last_checkpoint_sequence = current_sequence;
    Ok(())
}

async fn append_wal(
    root: &Path,
    fragment: &str,
    partition_id: u16,
    record: &IndexV2WalRecord,
) -> Result<u64, AppError> {
    let mut manifest = load_wal_manifest(root, fragment, partition_id).await?;
    let active_path = wal_segment_path(root, fragment, partition_id, manifest.active_segment_id);
    let should_rotate = fs::metadata(&active_path)
        .await
        .ok()
        .map(|meta| meta.len() >= INDEX_V2_WAL_SEGMENT_MAX_BYTES)
        .unwrap_or(false);
    if should_rotate {
        manifest.active_segment_id += 1;
    }

    let sequence = manifest.next_sequence;
    let envelope = IndexV2WalEnvelope {
        sequence,
        recorded_at: Utc::now().timestamp(),
        record: record.clone(),
    };
    append_length_prefixed_record(
        &wal_segment_path(root, fragment, partition_id, manifest.active_segment_id),
        &envelope,
    )
    .await?;
    manifest.next_sequence += 1;
    maybe_write_checkpoint(root, fragment, partition_id, &mut manifest).await?;
    persist_wal_manifest(root, fragment, partition_id, &manifest).await?;
    Ok(sequence)
}

async fn read_current_checkpoint(
    root: &Path,
    fragment: &str,
    partition_id: u16,
) -> Result<Option<IndexV2Checkpoint>, AppError> {
    let manifest = load_wal_manifest(root, fragment, partition_id).await?;
    let latest_sequence = manifest.next_sequence.saturating_sub(1);
    let Some(checkpoint) =
        read_messagepack_if_exists::<IndexV2Checkpoint>(&latest_checkpoint_path(
            root,
            fragment,
            partition_id,
        ))
        .await?
    else {
        return Ok(None);
    };
    if checkpoint.up_to_sequence == latest_sequence {
        return Ok(Some(checkpoint));
    }
    Ok(None)
}

async fn read_wal_records_since(
    root: &Path,
    fragment: &str,
    partition_id: u16,
    after_sequence: u64,
) -> Result<Vec<IndexV2WalEnvelope>, AppError> {
    let manifest = load_wal_manifest(root, fragment, partition_id).await?;
    let latest_sequence = manifest.next_sequence.saturating_sub(1);
    if latest_sequence <= after_sequence {
        return Ok(Vec::new());
    }

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
        let records = decode_length_prefixed_records::<IndexV2WalEnvelope>(&bytes, &path)?;
        out.extend(records.into_iter().filter(|record| record.sequence > after_sequence));
    }
    Ok(out)
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
    let delimiter_segment =
        build_delimiter_compacted_from_entries(prefix, &prefix_segment.entries);
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
        let delimiter_ok = write_atomic(&delimiter_path, &delimiter_bytes).await.is_ok();
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
                if (name.starts_with("seg_")
                    || name.starts_with(PREFIX_DELIMITER_SEGMENT_PREFIX))
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
                "index_v2 prefix snapshot read failed {}: {}",
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
            Err(e) => warn!(
                "index_v2 prefix cursor read failed {}: {}",
                path.display(),
                e
            ),
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
                "index_v2 prefix cursor persist failed {}: {}",
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

async fn project_fragment_prefixes(fragment: &str) -> Result<(), AppError> {
    let path_manager = ObjectPathManager::new();
    let roots = path_manager.get_all_index_roots();
    let mut cursor = load_prefix_projection_cursor(fragment).await?;
    let mut changed_keys = HashSet::<String>::new();

    for root in roots {
        let partitions_root = root
            .join(fragment)
            .join(INDEX_V2_DIR)
            .join(INDEX_V2_PARTITIONS_DIR);
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
            let records =
                match read_wal_records_since(&root, fragment, partition_id, after_sequence).await {
                    Ok(records) => records,
                    Err(e) => {
                        warn!(
                            "index_v2 prefix projector wal read failed fragment={} root={} partition={:04x}: {}",
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
                    IndexV2WalRecord::VersionPublished { metadata, .. }
                    | IndexV2WalRecord::VersionTrashed { metadata, .. } => {
                        changed_keys.insert(canonical_key(&metadata.object_key));
                    }
                    IndexV2WalRecord::VersionRemoved { object_key, .. } => {
                        changed_keys.insert(canonical_key(&object_key));
                    }
                    IndexV2WalRecord::SessionOpened { .. }
                    | IndexV2WalRecord::PartCommitted { .. }
                    | IndexV2WalRecord::SessionRemoved { .. } => {}
                }
            }
            if max_sequence > after_sequence {
                cursor.sources.insert(source_id, max_sequence);
            }
        }
    }

    if changed_keys.is_empty() {
        return persist_prefix_projection_cursor(fragment, &cursor).await;
    }

    let affected_prefixes = changed_keys
        .iter()
        .flat_map(|key| prefixes_for_key(key))
        .map(|prefix| canonical_prefix(&prefix))
        .collect::<BTreeSet<_>>();
    let mut live_entries = HashMap::<String, Option<PrefixIndexEntry>>::new();
    for key in &changed_keys {
        let current = match read_latest_object(fragment, key).await {
            Ok(metadata) if !metadata.is_delete_marker => Some(PrefixIndexEntry::from_metadata(&metadata)),
            Ok(_) => None,
            Err(AppError::NotFound) => None,
            Err(e) => {
                warn!(
                    "index_v2 prefix projection latest read failed fragment={} key={}: {}",
                    fragment,
                    key,
                    e
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

        if let Err(e) = write_prefix_compacted_snapshot(&path_manager, fragment, &prefix, entries).await {
            write_errors += 1;
            warn!(
                "index_v2 prefix projection write failed fragment={} prefix='{}': {}",
                fragment,
                prefix,
                e
            );
        }
    }

    if write_errors > 0 {
        return Err(AppError::InternalServerError(format!(
            "prefix projection failed for {} prefixes",
            write_errors
        )));
    }

    persist_prefix_projection_cursor(fragment, &cursor).await
}

#[cfg(test)]
fn schedule_prefix_projection(fragment: String) {
    tokio::task::block_in_place(|| {
        let handle = tokio::runtime::Handle::current();
        handle.block_on(async move {
            if let Err(e) = project_fragment_prefixes(&fragment).await {
                warn!(
                    "index_v2 prefix projection task failed fragment={}: {}",
                    fragment,
                    e
                );
            }
        });
    });
}

#[cfg(not(test))]
fn schedule_prefix_projection(fragment: String) {
    tokio::spawn(async move {
        let should_start = {
            let mut tasks = PREFIX_PROJECTION_TASKS.lock().await;
            let state = tasks.entry(fragment.clone()).or_default();
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
            let _permit = match PREFIX_PROJECTION_IO_GATE.acquire().await {
                Ok(permit) => permit,
                Err(_) => break,
            };
            let projection_result = project_fragment_prefixes(&fragment).await;
            if let Err(e) = &projection_result {
                warn!(
                    "index_v2 prefix projection task failed fragment={}: {}",
                    fragment,
                    e
                );
            }

            let should_rerun = {
                let mut tasks = PREFIX_PROJECTION_TASKS.lock().await;
                match tasks.get_mut(&fragment) {
                    Some(state) => {
                        let rerun = state.rerun_requested || projection_result.is_err();
                        state.rerun_requested = false;
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

pub async fn is_prefix_projection_pending(fragment: &str) -> bool {
    PREFIX_PROJECTION_TASKS
        .lock()
        .await
        .get(fragment)
        .map(|state| state.running || state.rerun_requested)
        .unwrap_or(false)
}

fn metadata_is_newer(candidate: &ObjectMetadata, current: &ObjectMetadata) -> bool {
    version_recency_is_newer(
        candidate.created_at,
        &candidate.version_id,
        current.created_at,
        &current.version_id,
    )
}

fn latest_marker_name() -> String {
    format!("latest{}", INDEX_V2_LATEST_SUFFIX)
}

fn is_version_entry(name: &str) -> bool {
    name.ends_with(INDEX_V2_OBJECT_SUFFIX) && name != latest_marker_name()
}

async fn load_latest_state_for_state(
    route: &IndexV2Route,
    fragment: &str,
    object_key: &str,
    trashed: bool,
) -> Option<IndexV2LatestState> {
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
        match read_messagepack::<IndexV2LatestState>(&path).await {
            Ok(state) => return Some(state),
            Err(e) => warn!("index_v2 latest read failed {}: {}", path.display(), e),
        }
    }
    None
}

async fn load_latest_state(
    route: &IndexV2Route,
    fragment: &str,
    object_key: &str,
) -> Option<IndexV2LatestState> {
    load_latest_state_for_state(route, fragment, object_key, false).await
}

async fn collect_object_versions_for_state(
    route: &IndexV2Route,
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
                warn!("index_v2 version dir read failed {}: {}", dir.display(), e);
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
            versions.entry(metadata.version_id.clone()).or_insert(metadata);
        }
    }
    versions.into_values().collect()
}

async fn collect_object_versions(
    route: &IndexV2Route,
    fragment: &str,
    object_key: &str,
) -> Vec<ObjectMetadata> {
    collect_object_versions_for_state(route, fragment, object_key, false).await
}

async fn persist_latest_state_for_state(
    route: &IndexV2Route,
    fragment: &str,
    object_key: &str,
    latest: Option<&IndexV2LatestState>,
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
            Err(e) => warn!("index_v2 latest persist failed on root {}: {}", root.display(), e),
        }
    }
    if success == 0 {
        return Err(AppError::InternalServerError(
            "index_v2 latest write failed on all roots".to_string(),
        ));
    }
    Ok(())
}

async fn write_object_version_internal(
    metadata: &ObjectMetadata,
    force_latest: bool,
    trashed: bool,
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
    let latest = should_update_latest.then(|| IndexV2LatestState {
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
        IndexV2WalRecord::VersionTrashed {
            metadata: metadata.clone(),
            trashed_at: Utc::now().timestamp(),
        }
    } else {
        IndexV2WalRecord::VersionPublished {
            metadata: metadata.clone(),
            published_at: Utc::now().timestamp(),
        }
    };

    write_to_selected_roots(&route.roots, |root| {
        let version_path = object_version_path_for_state(
            root,
            &metadata.fragment,
            route.partition_id,
            &metadata.object_key,
            &metadata.version_id,
            trashed,
        );
        let latest_path = latest_state_path_for_state(
            root,
            &fragment,
            route.partition_id,
            &metadata.object_key,
            trashed,
        );
        let version_bytes = version_bytes.clone();
        let latest_bytes = latest_bytes.clone();
        let wal = wal.clone();
        let fragment = fragment.clone();
        Box::pin(async move {
            write_atomic(&version_path, &version_bytes).await?;
            if let Some(bytes) = &latest_bytes {
                write_atomic(&latest_path, bytes).await?;
            }
            append_wal(root, &fragment, route.partition_id, &wal)
                .await
                .map(|_| ())
        })
    })
    .await?;
    Ok(())
}

async fn delete_object_version_internal(
    fragment: &str,
    object_key: &str,
    version_id: &str,
    trashed: bool,
) -> Result<bool, AppError> {
    let route = object_partition_route(fragment, object_key)?;
    let mut removed_any = false;
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
        match fs::remove_file(&path).await {
            Ok(()) => {
                if let Some(parent) = path.parent() {
                    let _ = sync_dir(parent).await;
                }
                removed_any = true;
            }
            Err(e) => warn!("index_v2 remove failed {}: {}", path.display(), e),
        }
    }

    if !removed_any {
        return Ok(false);
    }

    let next_latest = collect_object_versions_for_state(&route, fragment, object_key, trashed)
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
        .map(|metadata| IndexV2LatestState {
            version_id: metadata.version_id.clone(),
            metadata,
        });
    persist_latest_state_for_state(&route, fragment, object_key, next_latest.as_ref(), trashed)
        .await?;

    let wal = IndexV2WalRecord::VersionRemoved {
        fragment: fragment.to_string(),
        object_key: object_key.to_string(),
        version_id: version_id.to_string(),
        removed_at: Utc::now().timestamp(),
        from_trash: trashed,
    };
    for root in &route.roots {
        let _ = append_wal(root, fragment, route.partition_id, &wal).await;
    }

    Ok(true)
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
    for (path, _) in replicas {
        match read_messagepack::<ObjectMetadata>(&path).await {
            Ok(metadata) => return Ok(metadata),
            Err(e) => warn!("index_v2 version read failed {}: {}", path.display(), e),
        }
    }
    Err(AppError::NotFound)
}

fn absorb_latest_state(
    latest_by_key: &mut HashMap<String, IndexV2LatestState>,
    candidate: IndexV2LatestState,
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
) -> Result<Vec<IndexV2LatestState>, AppError> {
    let roots = ObjectPathManager::new().get_all_index_roots();
    let mut latest_by_key = HashMap::<String, IndexV2LatestState>::new();
    let prefix = prefix.map(canonical_prefix);

    for root in roots {
        let partitions_root = root
            .join(fragment)
            .join(INDEX_V2_DIR)
            .join(INDEX_V2_PARTITIONS_DIR);
        if !fs::try_exists(&partitions_root).await.unwrap_or(false) {
            continue;
        }

        let mut partitions = match fs::read_dir(&partitions_root).await {
            Ok(entries) => entries,
            Err(e) => {
                warn!(
                    "index_v2 latest scan failed to read {}: {}",
                    partitions_root.display(),
                    e
                );
                continue;
            }
        };

        while let Ok(Some(partition)) = partitions.next_entry().await {
            let partition_name = partition.file_name().to_string_lossy().to_string();
            let Ok(partition_id) = u16::from_str_radix(&partition_name, 16) else {
                continue;
            };

            if let Some(checkpoint) = read_current_checkpoint(&root, fragment, partition_id).await? {
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

            for state in collect_partition_latest_states_on_root(&root, fragment, partition_id, trashed)
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
) -> Result<Vec<IndexV2LatestState>, AppError> {
    scan_fragment_latest_states_filtered(fragment, trashed, None).await
}

fn absorb_version_state(
    versions: &mut HashMap<(String, String), ObjectMetadata>,
    candidate: ObjectMetadata,
) {
    let key = (candidate.object_key.clone(), candidate.version_id.clone());
    match versions.get(&key) {
        None => {
            versions.insert(key, candidate);
        }
        Some(current) => {
            if metadata_is_newer(&candidate, current) {
                versions.insert(key, candidate);
            }
        }
    }
}

async fn scan_fragment_versions(
    fragment: &str,
    trashed: bool,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let roots = ObjectPathManager::new().get_all_index_roots();
    let mut versions = HashMap::<(String, String), ObjectMetadata>::new();

    for root in roots {
        let partitions_root = root
            .join(fragment)
            .join(INDEX_V2_DIR)
            .join(INDEX_V2_PARTITIONS_DIR);
        if !fs::try_exists(&partitions_root).await.unwrap_or(false) {
            continue;
        }

        let mut partitions = match fs::read_dir(&partitions_root).await {
            Ok(entries) => entries,
            Err(e) => {
                warn!(
                    "index_v2 version scan failed to read {}: {}",
                    partitions_root.display(),
                    e
                );
                continue;
            }
        };

        while let Ok(Some(partition)) = partitions.next_entry().await {
            let objects_root = partition.path().join(if trashed {
                INDEX_V2_TRASH_DIR
            } else {
                INDEX_V2_OBJECTS_DIR
            });
            if !fs::try_exists(&objects_root).await.unwrap_or(false) {
                continue;
            }

            let mut bucket_dirs = match fs::read_dir(&objects_root).await {
                Ok(entries) => entries,
                Err(_) => continue,
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
                        match read_messagepack::<ObjectMetadata>(&file.path()).await {
                            Ok(metadata) => absorb_version_state(&mut versions, metadata),
                            Err(e) => warn!(
                                "index_v2 version scan decode failed {}: {}",
                                file.path().display(),
                                e
                            ),
                        }
                    }
                }
            }
        }
    }

    Ok(versions.into_values().collect())
}

async fn write_to_selected_roots<F>(
    roots: &[PathBuf],
    mut write_fn: F,
) -> Result<usize, AppError>
where
    F: FnMut(&Path) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), AppError>> + Send + '_>>,
{
    let mut success = 0usize;
    for root in roots {
        match write_fn(root.as_path()).await {
            Ok(()) => success += 1,
            Err(e) => warn!("index_v2 write failed on root {}: {}", root.display(), e),
        }
    }
    if success == 0 {
        return Err(AppError::InternalServerError(
            "index_v2 write failed on all roots".to_string(),
        ));
    }
    Ok(success)
}

pub async fn open_session(
    manifest: &UploadSessionManifest,
    reservation: Option<ReservationReceipt>,
) -> Result<(), AppError> {
    let route = session_partition_route(&manifest.fragment, &manifest.version_id)?;
    let state = IndexV2SessionState {
        manifest: manifest.clone(),
        reservation,
        parts: BTreeMap::new(),
        finalized_at: None,
        aborted_at: None,
    };
    let state_bytes = rmp_serde::to_vec(&state)
        .map_err(|e| AppError::InternalServerError(format!("serialize session: {}", e)))?;
    let fragment = manifest.fragment.clone();
    let version_id = manifest.version_id.clone();
    let wal = IndexV2WalRecord::SessionOpened {
        session: state.clone(),
    };
    write_to_selected_roots(&route.roots, |root| {
        let state_path = session_state_path(root, &fragment, route.partition_id, &version_id);
        let bytes = state_bytes.clone();
        let wal = wal.clone();
        let fragment = fragment.clone();
        Box::pin(async move {
            write_atomic(&state_path, &bytes).await?;
            append_wal(root, &fragment, route.partition_id, &wal)
                .await
                .map(|_| ())
        })
    })
    .await?;
    Ok(())
}

pub async fn load_session(
    fragment: &str,
    version_id: &str,
) -> Result<Option<IndexV2SessionState>, AppError> {
    let route = session_partition_route(fragment, version_id)?;
    let mut replicas = Vec::<(PathBuf, SystemTime)>::new();
    for root in &route.roots {
        let path = session_state_path(root, fragment, route.partition_id, version_id);
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
        match read_messagepack::<IndexV2SessionState>(&path).await {
            Ok(state) => return Ok(Some(state)),
            Err(e) => warn!("index_v2 session read failed {}: {}", path.display(), e),
        }
    }
    Ok(None)
}

pub async fn commit_part(
    fragment: &str,
    version_id: &str,
    part_meta: &PartMetadata,
) -> Result<bool, AppError> {
    let route = session_partition_route(fragment, version_id)?;
    let mut state = load_session(fragment, version_id)
        .await?
        .ok_or(AppError::NotFound)?;
    if state.parts.contains_key(&part_meta.part_index) {
        return Ok(false);
    }
    state.parts.insert(part_meta.part_index, part_meta.clone());
    let state_bytes = rmp_serde::to_vec(&state)
        .map_err(|e| AppError::InternalServerError(format!("serialize session: {}", e)))?;
    let fragment = fragment.to_string();
    let version_id = version_id.to_string();
    let wal = IndexV2WalRecord::PartCommitted {
        fragment: fragment.clone(),
        version_id: version_id.clone(),
        part_index: part_meta.part_index,
        part: part_meta.clone(),
        committed_at: Utc::now().timestamp(),
    };
    write_to_selected_roots(&route.roots, |root| {
        let state_path = session_state_path(root, &fragment, route.partition_id, &version_id);
        let bytes = state_bytes.clone();
        let wal = wal.clone();
        let fragment = fragment.clone();
        Box::pin(async move {
            write_atomic(&state_path, &bytes).await?;
            append_wal(root, &fragment, route.partition_id, &wal)
                .await
                .map(|_| ())
        })
    })
    .await?;
    Ok(true)
}

pub async fn remove_session(fragment: &str, version_id: &str, reason: &str) -> Result<(), AppError> {
    let route = session_partition_route(fragment, version_id)?;
    let wal = IndexV2WalRecord::SessionRemoved {
        fragment: fragment.to_string(),
        version_id: version_id.to_string(),
        removed_at: Utc::now().timestamp(),
        reason: reason.to_string(),
    };
    let mut success = 0usize;
    for root in &route.roots {
        let path = session_state_path(root, fragment, route.partition_id, version_id);
        if fs::try_exists(&path).await.unwrap_or(false) {
            if fs::remove_file(&path).await.is_ok() {
                if let Some(parent) = path.parent() {
                    let _ = sync_dir(parent).await;
                }
                success += 1;
            }
        }
        let _ = append_wal(root, fragment, route.partition_id, &wal).await;
    }
    if success == 0 {
        return Err(AppError::NotFound);
    }
    Ok(())
}

pub async fn list_active_sessions(fragment: &str) -> Result<Vec<IndexV2SessionState>, AppError> {
    let roots = ObjectPathManager::new().get_all_index_roots();
    let mut sessions = HashMap::<String, IndexV2SessionState>::new();

    for root in roots {
        let partitions_root = root
            .join(fragment)
            .join(INDEX_V2_DIR)
            .join(INDEX_V2_PARTITIONS_DIR);
        if !fs::try_exists(&partitions_root).await.unwrap_or(false) {
            continue;
        }
        let mut partitions = match fs::read_dir(&partitions_root).await {
            Ok(v) => v,
            Err(_) => continue,
        };
        while let Ok(Some(partition)) = partitions.next_entry().await {
            let partition_name = partition.file_name().to_string_lossy().to_string();
            let Ok(partition_id) = u16::from_str_radix(&partition_name, 16) else {
                continue;
            };

            let partition_sessions = if let Some(checkpoint) =
                read_current_checkpoint(&root, fragment, partition_id).await?
            {
                checkpoint.sessions
            } else {
                collect_partition_sessions_on_root(&root, fragment, partition_id).await?
            };

            for state in partition_sessions {
                if state.finalized_at.is_some() || state.aborted_at.is_some() {
                    continue;
                }
                sessions
                    .entry(state.manifest.version_id.clone())
                    .or_insert(state);
            }
        }
    }

    Ok(sessions.into_values().collect())
}

pub fn get_session_state_paths(fragment: &str, version_id: &str) -> Result<Vec<PathBuf>, AppError> {
    let route = session_partition_route(fragment, version_id)?;
    Ok(route
        .roots
        .iter()
        .map(|root| session_state_path(root, fragment, route.partition_id, version_id))
        .collect())
}

pub async fn publish_object(metadata: &ObjectMetadata) -> Result<(), AppError> {
    write_object_version_internal(metadata, false, false).await?;
    schedule_prefix_projection(metadata.fragment.clone());
    Ok(())
}

pub async fn upsert_object_version(metadata: &ObjectMetadata) -> Result<(), AppError> {
    write_object_version_internal(metadata, false, false).await?;
    schedule_prefix_projection(metadata.fragment.clone());
    Ok(())
}

pub async fn upsert_trashed_object_version(metadata: &ObjectMetadata) -> Result<(), AppError> {
    write_object_version_internal(metadata, false, true).await?;
    schedule_prefix_projection(metadata.fragment.clone());
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

pub async fn list_fragment_object_versions(
    fragment: &str,
    trashed: bool,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let mut versions = scan_fragment_versions(fragment, trashed).await?;
    versions.sort_by(|left, right| {
        left.object_key
            .cmp(&right.object_key)
            .then(left.version_id.cmp(&right.version_id))
    });
    Ok(versions)
}

pub async fn delete_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<bool, AppError> {
    let removed = delete_object_version_internal(fragment, object_key, version_id, false).await?;
    if removed {
        schedule_prefix_projection(fragment.to_string());
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
        schedule_prefix_projection(fragment.to_string());
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

    write_object_version_internal(&metadata, true, true).await?;
    delete_object_version_internal(fragment, object_key, version_id, false).await?;
    schedule_prefix_projection(fragment.to_string());
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

    write_object_version_internal(&metadata, force_latest, false).await?;
    delete_object_version_internal(fragment, object_key, version_id, true).await?;
    schedule_prefix_projection(fragment.to_string());
    Ok(Some(metadata))
}

pub async fn read_latest_object(fragment: &str, object_key: &str) -> Result<ObjectMetadata, AppError> {
    let route = object_partition_route(fragment, object_key)?;
    let latest = load_latest_state(&route, fragment, object_key)
        .await
        .ok_or(AppError::NotFound)?;

    // Latest markers are an acceleration structure. Rehydrate the named version
    // before serving so stale cached latest payload cannot hide repaired or newer
    // authoritative version metadata.
    match read_object_version_for_state(fragment, object_key, &latest.version_id, false).await {
        Ok(metadata) => Ok(metadata),
        Err(AppError::NotFound) => Ok(latest.metadata),
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
