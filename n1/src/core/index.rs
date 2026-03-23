use crate::enums::app_error_enum::AppError;
use crate::core::disk_manager::{
    ListObjectsResult, ObjectMetadata, ObjectPathManager, PartMetadata, ReservationReceipt,
};
use crate::core::upload::UploadSessionManifest;
use base64::Engine;
use base64::engine::general_purpose;
use chrono::Utc;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use ulid::Ulid;

const INDEX_DIR: &str = "index";
const INDEX_PARTITIONS_DIR: &str = "partitions";
const INDEX_OBJECTS_DIR: &str = "objects";
const INDEX_TRASH_DIR: &str = "trash";
const INDEX_SESSIONS_DIR: &str = "sessions";
const INDEX_PARTITION_COUNT: u16 = 256;
const INDEX_LATEST_FILE: &str = "latest.msgpack";
const INDEX_OBJECT_SUFFIX: &str = ".msgpack";
const INDEX_SESSION_SUFFIX: &str = ".session.msgpack";
const INDEX_SESSION_REMOVED_SUFFIX: &str = ".removed.msgpack";
const INDEX_PART_META_SUFFIX: &str = ".meta.msgpack";
const SESSION_MANIFEST_FILE: &str = "session.manifest.msgpack";
const SESSION_PART_RECEIPTS_FILE: &str = "part.receipts.bin";
const RESERVATION_FILE: &str = "reservation.msgpack";
const SESSION_PART_RECEIPTS_MAGIC: &[u8; 8] = b"N1PRT001";
const SESSION_PART_RECEIPTS_HEADER_SIZE: usize = 24;
const SESSION_PART_RECEIPTS_SLOT_BASE_SIZE: usize = 28;
const SESSION_PART_RECEIPTS_STATUS_COMMITTED: u8 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexReplicaWriteMode {
    BackgroundBestEffort,
    SyncBestEffort,
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionPartReceipt {
    pub part_index: u64,
    pub size_bytes: u64,
    pub created_at: i64,
    pub checksum: Option<u32>,
    pub shard_checksums: Vec<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexLatestState {
    version_id: String,
    metadata: ObjectMetadata,
}

#[derive(Debug, Clone)]
struct IndexRoute {
    partition_id: u16,
    roots: Vec<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FragmentScanCursor {
    partition_id: u16,
    bucket_id: u8,
    after_hash: String,
}

fn canonical_key(object_key: &str) -> String {
    ObjectPathManager::canonical_object_key(object_key)
}

fn is_internal_object_key(object_key: &str) -> bool {
    let key = canonical_key(object_key);
    key.split('/')
        .any(|segment| segment == ".n1fsd_dir" || segment == ".n1-trash")
}

fn encode_fragment_scan_token(cursor: &FragmentScanCursor) -> String {
    general_purpose::STANDARD.encode(format!(
        "scan|{:04x}|{:02x}|{}",
        cursor.partition_id, cursor.bucket_id, cursor.after_hash
    ))
}

fn decode_fragment_scan_token(token: &str) -> Option<FragmentScanCursor> {
    let bytes = general_purpose::STANDARD.decode(token).ok()?;
    let raw = String::from_utf8(bytes).ok()?;
    let mut parts = raw.splitn(4, '|');
    let kind = parts.next()?;
    if kind != "scan" {
        return None;
    }
    let partition_id = u16::from_str_radix(parts.next()?, 16).ok()?;
    let bucket_id = u8::from_str_radix(parts.next()?, 16).ok()?;
    let after_hash = parts.next()?.to_string();
    Some(FragmentScanCursor {
        partition_id,
        bucket_id,
        after_hash,
    })
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
        // Deterministic replica root, distinct from owner.
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

fn partition_dir(root: &Path, fragment: &str, partition_id: u16) -> PathBuf {
    root.join(fragment)
        .join(INDEX_DIR)
        .join(INDEX_PARTITIONS_DIR)
        .join(format!("{:04x}", partition_id))
}

fn objects_dir_for_partition(
    root: &Path,
    fragment: &str,
    partition_id: u16,
    trashed: bool,
) -> PathBuf {
    partition_dir(root, fragment, partition_id).join(if trashed {
        INDEX_TRASH_DIR
    } else {
        INDEX_OBJECTS_DIR
    })
}

fn object_base_dir(
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

fn object_version_path(
    root: &Path,
    fragment: &str,
    partition_id: u16,
    object_key: &str,
    version_id: &str,
    trashed: bool,
) -> PathBuf {
    object_base_dir(root, fragment, partition_id, object_key, trashed)
        .join(format!("{}{}", version_id, INDEX_OBJECT_SUFFIX))
}

fn object_latest_path(
    root: &Path,
    fragment: &str,
    partition_id: u16,
    object_key: &str,
    trashed: bool,
) -> PathBuf {
    object_base_dir(root, fragment, partition_id, object_key, trashed).join(INDEX_LATEST_FILE)
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

fn session_part_path(
    root: &Path,
    fragment: &str,
    partition_id: u16,
    version_id: &str,
    part_index: u64,
) -> PathBuf {
    session_parts_dir(root, fragment, partition_id, version_id)
        .join(format!("part_{}{}", part_index, INDEX_PART_META_SUFFIX))
}

fn removed_session_receipt_path(
    root: &Path,
    fragment: &str,
    partition_id: u16,
    version_id: &str,
) -> PathBuf {
    partition_dir(root, fragment, partition_id)
        .join(INDEX_SESSIONS_DIR)
        .join(format!("{}{}", version_id, INDEX_SESSION_REMOVED_SUFFIX))
}

fn session_compat_dir(root: &Path, fragment: &str, partition_id: u16, version_id: &str) -> PathBuf {
    partition_dir(root, fragment, partition_id)
        .join(INDEX_SESSIONS_DIR)
        .join(version_id)
}

fn session_part_receipts_path(
    root: &Path,
    fragment: &str,
    partition_id: u16,
    version_id: &str,
) -> PathBuf {
    session_compat_dir(root, fragment, partition_id, version_id).join(SESSION_PART_RECEIPTS_FILE)
}

fn session_receipt_total_shards(manifest: &UploadSessionManifest) -> Option<usize> {
    let total = usize::from(manifest.data_shards) + usize::from(manifest.parity_shards);
    if total == 0 { None } else { Some(total) }
}

fn session_receipt_slot_size(total_shards: usize) -> Result<usize, AppError> {
    SESSION_PART_RECEIPTS_SLOT_BASE_SIZE
        .checked_add(total_shards.checked_mul(4).ok_or_else(|| {
            AppError::InternalServerError("receipt slot size overflow".to_string())
        })?)
        .ok_or_else(|| AppError::InternalServerError("receipt slot size overflow".to_string()))
}

fn session_receipt_table_len(expected_parts: u64, slot_size: usize) -> Result<u64, AppError> {
    let payload_len = (expected_parts as u128)
        .checked_mul(slot_size as u128)
        .ok_or_else(|| AppError::InternalServerError("receipt table size overflow".to_string()))?;
    let total_len = (SESSION_PART_RECEIPTS_HEADER_SIZE as u128)
        .checked_add(payload_len)
        .ok_or_else(|| AppError::InternalServerError("receipt table size overflow".to_string()))?;
    u64::try_from(total_len)
        .map_err(|_| AppError::InternalServerError("receipt table size overflow".to_string()))
}

fn encode_session_receipt_header(
    manifest: &UploadSessionManifest,
    total_shards: usize,
    slot_size: usize,
) -> Result<[u8; SESSION_PART_RECEIPTS_HEADER_SIZE], AppError> {
    let total_shards_u32 = u32::try_from(total_shards)
        .map_err(|_| AppError::InternalServerError("total shard count too large".to_string()))?;
    let slot_size_u32 = u32::try_from(slot_size)
        .map_err(|_| AppError::InternalServerError("receipt slot size too large".to_string()))?;

    let mut header = [0u8; SESSION_PART_RECEIPTS_HEADER_SIZE];
    header[0..8].copy_from_slice(SESSION_PART_RECEIPTS_MAGIC);
    header[8..12].copy_from_slice(&total_shards_u32.to_le_bytes());
    header[12..20].copy_from_slice(&manifest.expected_parts.to_le_bytes());
    header[20..24].copy_from_slice(&slot_size_u32.to_le_bytes());
    Ok(header)
}

fn parse_crc32_hex(value: &str) -> Result<u32, AppError> {
    u32::from_str_radix(value.trim(), 16).map_err(|e| {
        AppError::InternalServerError(format!("invalid crc32 value '{}': {}", value, e))
    })
}

fn encode_session_part_receipt(
    part: &PartMetadata,
    total_shards: usize,
    slot_size: usize,
) -> Result<Vec<u8>, AppError> {
    if part.shards.len() != total_shards {
        return Err(AppError::InternalServerError(format!(
            "part {} has {} shards, expected {}",
            part.part_index,
            part.shards.len(),
            total_shards
        )));
    }

    let mut slot = vec![0u8; slot_size];
    slot[0] = SESSION_PART_RECEIPTS_STATUS_COMMITTED;
    slot[1] = u8::from(part.checksum.is_some());
    slot[8..16].copy_from_slice(&part.size_bytes.to_le_bytes());
    slot[16..24].copy_from_slice(&part.created_at.to_le_bytes());

    let whole_checksum = match part.checksum.as_deref() {
        Some(checksum) => parse_crc32_hex(checksum)?,
        None => 0,
    };
    slot[24..28].copy_from_slice(&whole_checksum.to_le_bytes());

    for (idx, shard) in part.shards.iter().enumerate() {
        let checksum = parse_crc32_hex(&shard.checksum)?;
        let start = SESSION_PART_RECEIPTS_SLOT_BASE_SIZE + idx * 4;
        slot[start..start + 4].copy_from_slice(&checksum.to_le_bytes());
    }

    Ok(slot)
}

fn decode_session_part_receipts(
    bytes: &[u8],
    manifest: &UploadSessionManifest,
) -> Result<Option<Vec<Option<SessionPartReceipt>>>, AppError> {
    if bytes.len() < SESSION_PART_RECEIPTS_HEADER_SIZE {
        return Ok(None);
    }
    if &bytes[0..8] != SESSION_PART_RECEIPTS_MAGIC {
        return Ok(None);
    }

    let total_shards = u32::from_le_bytes(bytes[8..12].try_into().unwrap_or([0u8; 4])) as usize;
    let expected_parts = u64::from_le_bytes(bytes[12..20].try_into().unwrap_or([0u8; 8]));
    let slot_size = u32::from_le_bytes(bytes[20..24].try_into().unwrap_or([0u8; 4])) as usize;

    if expected_parts != manifest.expected_parts {
        return Ok(None);
    }

    if let Some(expected_total_shards) = session_receipt_total_shards(manifest) {
        if total_shards != expected_total_shards {
            return Ok(None);
        }
    }

    let min_slot_size = session_receipt_slot_size(total_shards)?;
    if slot_size < min_slot_size {
        return Ok(None);
    }

    let expected_len = session_receipt_table_len(expected_parts, slot_size)? as usize;
    if bytes.len() < expected_len {
        return Ok(None);
    }

    let mut receipts = vec![None; expected_parts as usize];
    for (idx, receipt) in receipts.iter_mut().enumerate() {
        let offset = SESSION_PART_RECEIPTS_HEADER_SIZE + idx * slot_size;
        let slot = &bytes[offset..offset + slot_size];
        if slot.first().copied().unwrap_or_default() != SESSION_PART_RECEIPTS_STATUS_COMMITTED {
            continue;
        }

        let has_whole_checksum = slot.get(1).copied().unwrap_or_default() != 0;
        let size_bytes = u64::from_le_bytes(slot[8..16].try_into().unwrap_or([0u8; 8]));
        let created_at = i64::from_le_bytes(slot[16..24].try_into().unwrap_or([0u8; 8]));
        let whole_checksum = u32::from_le_bytes(slot[24..28].try_into().unwrap_or([0u8; 4]));

        let mut shard_checksums = Vec::with_capacity(total_shards);
        for shard_index in 0..total_shards {
            let start = SESSION_PART_RECEIPTS_SLOT_BASE_SIZE + shard_index * 4;
            let checksum =
                u32::from_le_bytes(slot[start..start + 4].try_into().unwrap_or([0u8; 4]));
            shard_checksums.push(checksum);
        }

        *receipt = Some(SessionPartReceipt {
            part_index: idx as u64,
            size_bytes,
            created_at,
            checksum: if has_whole_checksum {
                Some(whole_checksum)
            } else {
                None
            },
            shard_checksums,
        });
    }

    Ok(Some(receipts))
}

async fn initialize_session_receipt_table(
    path: PathBuf,
    manifest: &UploadSessionManifest,
    total_shards: usize,
    slot_size: usize,
) -> Result<(), AppError> {
    let table_len = session_receipt_table_len(manifest.expected_parts, slot_size)?;
    let header = encode_session_receipt_header(manifest, total_shards, slot_size)?;
    let error_path = path.clone();

    tokio::task::spawn_blocking(move || {
        use std::io::{Seek, SeekFrom, Write};

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)?;
        file.write_all(&header)?;
        file.set_len(table_len)?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&header)?;
        Ok::<(), std::io::Error>(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("receipt table init task failed: {}", e)))?
    .map_err(|e| {
        AppError::Io(
            e,
            format!("initialize receipt table {}", error_path.display()),
        )
    })
}

async fn write_session_receipt_slot(
    path: PathBuf,
    offset: u64,
    slot: Vec<u8>,
) -> Result<(), AppError> {
    let error_path = path.clone();
    tokio::task::spawn_blocking(move || {
        use std::io::{Seek, SeekFrom, Write};

        let mut file = std::fs::OpenOptions::new().write(true).open(&path)?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&slot)?;
        Ok::<(), std::io::Error>(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("receipt slot write task failed: {}", e)))?
    .map_err(|e| AppError::Io(e, format!("write receipt slot {}", error_path.display())))
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
    let dir = fs::OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
        .await
        .map_err(|e| AppError::Io(e, format!("open dir for sync: {}", path.display())))?;
    dir.sync_all()
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
        AppError::InternalServerError(format!("No parent for {}", path.display()))
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

async fn read_messagepack<T: DeserializeOwned>(path: &Path) -> Result<T, AppError> {
    let bytes = fs::read(path)
        .await
        .map_err(|e| AppError::Io(e, format!("read {}", path.display())))?;
    rmp_serde::from_slice(&bytes)
        .map_err(|e| AppError::InternalServerError(format!("decode {}: {}", path.display(), e)))
}

async fn read_messagepack_if_exists<T: DeserializeOwned>(
    path: &Path,
) -> Result<Option<T>, AppError> {
    if !fs::try_exists(path).await.unwrap_or(false) {
        return Ok(None);
    }
    read_messagepack(path).await.map(Some)
}

async fn list_version_ids_from_object_dir(dir: &Path) -> Result<Vec<String>, AppError> {
    let mut version_ids = HashSet::<String>::new();
    if !fs::try_exists(dir).await.unwrap_or(false) {
        return Ok(Vec::new());
    }

    let mut entries = fs::read_dir(dir)
        .await
        .map_err(|e| AppError::Io(e, format!("read dir {}", dir.display())))?;
    while let Ok(Some(entry)) = entries.next_entry().await {
        if !entry
            .file_type()
            .await
            .map(|ft| ft.is_file())
            .unwrap_or(false)
        {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.ends_with(INDEX_OBJECT_SUFFIX) || name == INDEX_LATEST_FILE {
            continue;
        }
        if let Some(version_id) = name.strip_suffix(INDEX_OBJECT_SUFFIX) {
            version_ids.insert(version_id.to_string());
        }
    }

    let mut out: Vec<String> = version_ids.into_iter().collect();
    out.sort_by(|left, right| compare_version_id_recency(left, right));
    Ok(out)
}

async fn remove_object_dir_fast_internal(
    fragment: &str,
    object_key: &str,
    trashed: bool,
) -> Result<Option<Vec<String>>, AppError> {
    let route = object_route(fragment, object_key)?;
    let mut version_ids = HashSet::<String>::new();
    let mut dirs_to_remove = Vec::<PathBuf>::new();

    for root in &route.roots {
        let dir = object_base_dir(root, fragment, route.partition_id, object_key, trashed);
        if !fs::try_exists(&dir).await.unwrap_or(false) {
            continue;
        }
        for version_id in list_version_ids_from_object_dir(&dir).await? {
            version_ids.insert(version_id);
        }
        dirs_to_remove.push(dir);
    }

    if dirs_to_remove.is_empty() {
        return Ok(None);
    }

    for dir in dirs_to_remove {
        fs::remove_dir_all(&dir)
            .await
            .map_err(|e| AppError::Io(e, format!("remove object dir {}", dir.display())))?;
    }

    let mut out: Vec<String> = version_ids.into_iter().collect();
    out.sort_by(|left, right| compare_version_id_recency(left, right));
    Ok(Some(out))
}

fn part_file_name_to_index(name: &str) -> Option<u64> {
    if !name.starts_with("part_") || !name.ends_with(INDEX_PART_META_SUFFIX) {
        return None;
    }
    let rest = name.strip_prefix("part_")?;
    let idx = rest.strip_suffix(INDEX_PART_META_SUFFIX)?;
    idx.parse::<u64>().ok()
}

async fn load_session_on_root(
    root: &Path,
    fragment: &str,
    partition_id: u16,
    version_id: &str,
) -> Result<Option<IndexSessionState>, AppError> {
    let state_path = session_state_path(root, fragment, partition_id, version_id);
    let mut state = match read_messagepack_if_exists::<IndexSessionState>(&state_path).await? {
        Some(state) => state,
        None => return Ok(None),
    };

    let mut parts = BTreeMap::<u64, PartMetadata>::new();
    let parts_dir = session_parts_dir(root, fragment, partition_id, version_id);
    if fs::try_exists(&parts_dir).await.unwrap_or(false) {
        let mut entries = match fs::read_dir(&parts_dir).await {
            Ok(entries) => entries,
            Err(_) => {
                state.parts = parts;
                return Ok(Some(state));
            }
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            if !entry
                .file_type()
                .await
                .map(|ft| ft.is_file())
                .unwrap_or(false)
            {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            let Some(part_index) = part_file_name_to_index(&name) else {
                continue;
            };
            if let Ok(part) = read_messagepack::<PartMetadata>(&entry.path()).await {
                parts.insert(part_index, part);
            }
        }
    }
    state.parts = parts;
    Ok(Some(state))
}

pub async fn open_session(
    manifest: &UploadSessionManifest,
    reservation: Option<ReservationReceipt>,
) -> Result<(), AppError> {
    let route = session_route(&manifest.fragment, &manifest.version_id)?;
    let state = IndexSessionState {
        manifest: manifest.clone(),
        reservation,
        parts: BTreeMap::new(),
        last_activity_at: manifest.created_at,
        finalized_at: None,
        aborted_at: None,
    };
    let bytes = rmp_serde::to_vec(&state)
        .map_err(|e| AppError::InternalServerError(format!("serialize session state: {}", e)))?;
    let manifest_bytes = rmp_serde::to_vec(manifest)
        .map_err(|e| AppError::InternalServerError(format!("serialize session manifest: {}", e)))?;
    let reservation_bytes = state
        .reservation
        .as_ref()
        .map(|r| {
            rmp_serde::to_vec(r).map_err(|e| {
                AppError::InternalServerError(format!("serialize reservation receipt: {}", e))
            })
        })
        .transpose()?;
    let receipt_layout = session_receipt_total_shards(manifest)
        .map(|total_shards| {
            session_receipt_slot_size(total_shards).map(|slot_size| (total_shards, slot_size))
        })
        .transpose()?;
    let mut success = 0usize;
    for root in &route.roots {
        let path = session_state_path(
            root,
            &manifest.fragment,
            route.partition_id,
            &manifest.version_id,
        );
        if write_atomic(&path, &bytes).await.is_ok() {
            success += 1;
        }
        let compat_dir = session_compat_dir(
            root,
            &manifest.fragment,
            route.partition_id,
            &manifest.version_id,
        );
        let manifest_path = compat_dir.join(SESSION_MANIFEST_FILE);
        let _ = write_atomic(&manifest_path, &manifest_bytes).await;
        match &reservation_bytes {
            Some(bytes) => {
                let _ = write_atomic(&compat_dir.join(RESERVATION_FILE), bytes).await;
            }
            None => {
                let _ = fs::remove_file(compat_dir.join(RESERVATION_FILE)).await;
            }
        }
        if let Some((total_shards, slot_size)) = receipt_layout {
            let _ = initialize_session_receipt_table(
                session_part_receipts_path(
                    root,
                    &manifest.fragment,
                    route.partition_id,
                    &manifest.version_id,
                ),
                manifest,
                total_shards,
                slot_size,
            )
            .await;
        }
        let removed = removed_session_receipt_path(
            root,
            &manifest.fragment,
            route.partition_id,
            &manifest.version_id,
        );
        let _ = fs::remove_file(removed).await;
    }
    if success == 0 {
        return Err(AppError::InternalServerError(
            "failed to persist session state on index roots".to_string(),
        ));
    }
    Ok(())
}

pub async fn commit_session_part(
    fragment: &str,
    version_id: &str,
    part: &PartMetadata,
    _replica_write_mode: IndexReplicaWriteMode,
) -> Result<(), AppError> {
    let route = session_route(fragment, version_id)?;
    let part_bytes = rmp_serde::to_vec(part)
        .map_err(|e| AppError::InternalServerError(format!("serialize part metadata: {}", e)))?;
    let mut success = 0usize;
    for root in &route.roots {
        let path = session_part_path(
            root,
            fragment,
            route.partition_id,
            version_id,
            part.part_index,
        );
        if write_atomic(&path, &part_bytes).await.is_ok() {
            success += 1;
        }
        let compat_dir = session_compat_dir(root, fragment, route.partition_id, version_id);
        let compat_part = compat_dir.join(format!(
            "part_{}{}",
            part.part_index, INDEX_PART_META_SUFFIX
        ));
        let _ = write_atomic(&compat_part, &part_bytes).await;
    }
    if success == 0 {
        return Err(AppError::InternalServerError(
            "failed to commit session part on index roots".to_string(),
        ));
    }
    Ok(())
}

pub async fn commit_session_part_receipt(
    fragment: &str,
    version_id: &str,
    manifest: &UploadSessionManifest,
    part: &PartMetadata,
) -> Result<(), AppError> {
    let Some(total_shards) = session_receipt_total_shards(manifest) else {
        return Ok(());
    };
    let slot_size = session_receipt_slot_size(total_shards)?;
    let slot = encode_session_part_receipt(part, total_shards, slot_size)?;
    let offset = (SESSION_PART_RECEIPTS_HEADER_SIZE as u64)
        .checked_add(
            (part.part_index as u64)
                .checked_mul(slot_size as u64)
                .ok_or_else(|| {
                    AppError::InternalServerError("receipt slot offset overflow".to_string())
                })?,
        )
        .ok_or_else(|| AppError::InternalServerError("receipt slot offset overflow".to_string()))?;

    let route = session_route(fragment, version_id)?;
    let mut success = 0usize;
    for root in &route.roots {
        let path = session_part_receipts_path(root, fragment, route.partition_id, version_id);
        if write_session_receipt_slot(path, offset, slot.clone())
            .await
            .is_ok()
        {
            success += 1;
        }
    }

    if success == 0 {
        return Err(AppError::InternalServerError(
            "failed to commit session part receipt on index roots".to_string(),
        ));
    }
    Ok(())
}

pub async fn load_session(
    fragment: &str,
    version_id: &str,
) -> Result<Option<IndexSessionState>, AppError> {
    let route = session_route(fragment, version_id)?;
    let mut candidates = Vec::<(IndexSessionState, SystemTime)>::new();
    for root in &route.roots {
        let state_path = session_state_path(root, fragment, route.partition_id, version_id);
        let modified = fs::metadata(&state_path)
            .await
            .ok()
            .and_then(|meta| meta.modified().ok())
            .unwrap_or(SystemTime::UNIX_EPOCH);
        if let Some(state) =
            load_session_on_root(root, fragment, route.partition_id, version_id).await?
        {
            candidates.push((state, modified));
        }
    }
    if candidates.is_empty() {
        return Ok(None);
    }

    candidates.sort_by(|left, right| {
        right
            .0
            .parts
            .len()
            .cmp(&left.0.parts.len())
            .then_with(|| right.0.last_activity_at.cmp(&left.0.last_activity_at))
            .then_with(|| right.1.cmp(&left.1))
    });
    Ok(candidates.into_iter().next().map(|(state, _)| state))
}

pub async fn read_session_part(
    fragment: &str,
    version_id: &str,
    part_index: u64,
) -> Result<Option<PartMetadata>, AppError> {
    let route = session_route(fragment, version_id)?;
    for root in &route.roots {
        let path = session_part_path(root, fragment, route.partition_id, version_id, part_index);
        if let Some(part) = read_messagepack_if_exists::<PartMetadata>(&path).await? {
            return Ok(Some(part));
        }
    }
    Ok(None)
}

pub async fn load_session_part_receipts(
    fragment: &str,
    version_id: &str,
    manifest: &UploadSessionManifest,
) -> Result<Option<Vec<Option<SessionPartReceipt>>>, AppError> {
    if session_receipt_total_shards(manifest).is_none() {
        return Ok(None);
    }

    let route = session_route(fragment, version_id)?;
    let mut best: Option<Vec<Option<SessionPartReceipt>>> = None;
    let mut best_count = 0usize;

    for root in &route.roots {
        let path = session_part_receipts_path(root, fragment, route.partition_id, version_id);
        if !fs::try_exists(&path).await.unwrap_or(false) {
            continue;
        }

        let bytes = match fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(_) => continue,
        };

        let Some(receipts) = decode_session_part_receipts(&bytes, manifest)? else {
            continue;
        };
        let count = receipts.iter().filter(|entry| entry.is_some()).count();
        if count > best_count {
            best_count = count;
            best = Some(receipts);
            if count == manifest.expected_parts as usize {
                break;
            }
        }
    }

    Ok(best)
}

pub async fn remove_session(
    fragment: &str,
    version_id: &str,
    reason: &str,
) -> Result<(), AppError> {
    let route = session_route(fragment, version_id)?;
    let snapshot = load_session(fragment, version_id).await?;
    let removed_at = Utc::now().timestamp();
    let mut changed = false;

    for root in &route.roots {
        let state_path = session_state_path(root, fragment, route.partition_id, version_id);
        if fs::try_exists(&state_path).await.unwrap_or(false) {
            let _ = fs::remove_file(&state_path).await;
            changed = true;
        }

        let parts_dir = session_parts_dir(root, fragment, route.partition_id, version_id);
        if fs::try_exists(&parts_dir).await.unwrap_or(false) {
            let _ = fs::remove_dir_all(&parts_dir).await;
            changed = true;
        }
        let compat_dir = session_compat_dir(root, fragment, route.partition_id, version_id);
        if fs::try_exists(&compat_dir).await.unwrap_or(false) {
            let _ = fs::remove_dir_all(&compat_dir).await;
            changed = true;
        }

        if let Some(state) = snapshot.clone() {
            let receipt = RemovedSessionReceipt {
                state,
                removed_at,
                reason: reason.to_string(),
            };
            if let Ok(bytes) = rmp_serde::to_vec(&receipt) {
                let path =
                    removed_session_receipt_path(root, fragment, route.partition_id, version_id);
                let _ = write_atomic(&path, &bytes).await;
            }
        }
    }

    if !changed && snapshot.is_none() {
        return Ok(());
    }
    Ok(())
}

pub async fn list_active_sessions(fragment: &str) -> Result<Vec<IndexSessionState>, AppError> {
    let roots = ObjectPathManager::new().get_all_index_roots();
    if roots.is_empty() {
        return Ok(Vec::new());
    }

    let mut by_version = HashMap::<String, IndexSessionState>::new();
    for partition_id in 0..INDEX_PARTITION_COUNT {
        for root in &roots {
            let sessions_dir = partition_dir(root, fragment, partition_id).join(INDEX_SESSIONS_DIR);
            if !fs::try_exists(&sessions_dir).await.unwrap_or(false) {
                continue;
            }

            let mut entries = match fs::read_dir(&sessions_dir).await {
                Ok(entries) => entries,
                Err(_) => continue,
            };
            while let Ok(Some(entry)) = entries.next_entry().await {
                if !entry
                    .file_type()
                    .await
                    .map(|ft| ft.is_file())
                    .unwrap_or(false)
                {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().to_string();
                if !name.ends_with(INDEX_SESSION_SUFFIX) {
                    continue;
                }
                let Some(version_id) = name.strip_suffix(INDEX_SESSION_SUFFIX) else {
                    continue;
                };
                let Some(state) =
                    load_session_on_root(root, fragment, partition_id, version_id).await?
                else {
                    continue;
                };

                match by_version.get(version_id) {
                    Some(existing)
                        if existing.last_activity_at > state.last_activity_at
                            || (existing.last_activity_at == state.last_activity_at
                                && existing.manifest.created_at >= state.manifest.created_at) => {}
                    _ => {
                        by_version.insert(version_id.to_string(), state);
                    }
                }
            }
        }
    }

    let mut out: Vec<IndexSessionState> = by_version
        .into_values()
        .filter(|state| state.finalized_at.is_none() && state.aborted_at.is_none())
        .collect();
    out.sort_by(|left, right| {
        right
            .manifest
            .created_at
            .cmp(&left.manifest.created_at)
            .then_with(|| right.manifest.version_id.cmp(&left.manifest.version_id))
    });
    Ok(out)
}

async fn upsert_object_version_internal(
    metadata: &ObjectMetadata,
    trashed: bool,
    _replica_write_mode: IndexReplicaWriteMode,
) -> Result<(), AppError> {
    let route = object_route(&metadata.fragment, &metadata.object_key)?;
    let version_bytes = rmp_serde::to_vec(metadata)
        .map_err(|e| AppError::InternalServerError(format!("serialize object metadata: {}", e)))?;
    let latest_state = IndexLatestState {
        version_id: metadata.version_id.clone(),
        metadata: metadata.clone(),
    };
    let latest_bytes = rmp_serde::to_vec(&latest_state)
        .map_err(|e| AppError::InternalServerError(format!("serialize latest state: {}", e)))?;

    let mut wrote_any = false;
    for root in &route.roots {
        let version_path = object_version_path(
            root,
            &metadata.fragment,
            route.partition_id,
            &metadata.object_key,
            &metadata.version_id,
            trashed,
        );
        if write_atomic(&version_path, &version_bytes).await.is_err() {
            continue;
        }
        wrote_any = true;

        let latest_path = object_latest_path(
            root,
            &metadata.fragment,
            route.partition_id,
            &metadata.object_key,
            trashed,
        );
        let should_update_latest =
            match read_messagepack_if_exists::<IndexLatestState>(&latest_path).await {
                Ok(Some(current)) => metadata_is_newer(metadata, &current.metadata),
                Ok(None) => true,
                Err(_) => true,
            };
        if should_update_latest {
            let _ = write_atomic(&latest_path, &latest_bytes).await;
        }
    }

    if !wrote_any {
        return Err(AppError::InternalServerError(
            "failed to persist object metadata on index roots".to_string(),
        ));
    }
    Ok(())
}

pub async fn publish_object(metadata: &ObjectMetadata) -> Result<(), AppError> {
    upsert_object_version_internal(metadata, false, IndexReplicaWriteMode::BackgroundBestEffort)
        .await
}

pub async fn upsert_object_version(metadata: &ObjectMetadata) -> Result<(), AppError> {
    upsert_object_version_internal(metadata, false, IndexReplicaWriteMode::BackgroundBestEffort)
        .await
}

pub async fn upsert_trashed_object_version(metadata: &ObjectMetadata) -> Result<(), AppError> {
    upsert_object_version_internal(metadata, true, IndexReplicaWriteMode::BackgroundBestEffort)
        .await
}

pub async fn read_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<ObjectMetadata, AppError> {
    read_object_version_internal(fragment, object_key, version_id, false).await
}

pub async fn read_trashed_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<ObjectMetadata, AppError> {
    read_object_version_internal(fragment, object_key, version_id, true).await
}

async fn read_object_version_internal(
    fragment: &str,
    object_key: &str,
    version_id: &str,
    trashed: bool,
) -> Result<ObjectMetadata, AppError> {
    let route = object_route(fragment, object_key)?;
    let mut replicas = Vec::<(SystemTime, PathBuf)>::new();
    for root in &route.roots {
        let path = object_version_path(
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
        replicas.push((modified, path));
    }

    replicas.sort_by(|left, right| right.0.cmp(&left.0));
    for (_modified, path) in replicas {
        if let Ok(mut metadata) = read_messagepack::<ObjectMetadata>(&path).await {
            if metadata.fragment != fragment {
                metadata.fragment = fragment.to_string();
            }
            return Ok(metadata);
        }
    }
    Err(AppError::NotFound)
}

async fn list_object_versions_metadata_internal(
    fragment: &str,
    object_key: &str,
    trashed: bool,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let route = object_route(fragment, object_key)?;
    let mut by_version = HashMap::<String, ObjectMetadata>::new();
    for root in &route.roots {
        let dir = object_base_dir(root, fragment, route.partition_id, object_key, trashed);
        if !fs::try_exists(&dir).await.unwrap_or(false) {
            continue;
        }
        let mut entries = match fs::read_dir(&dir).await {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            if !entry
                .file_type()
                .await
                .map(|ft| ft.is_file())
                .unwrap_or(false)
            {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.ends_with(INDEX_OBJECT_SUFFIX) || name == INDEX_LATEST_FILE {
                continue;
            }
            let Some(version_id) = name.strip_suffix(INDEX_OBJECT_SUFFIX) else {
                continue;
            };
            if let Ok(mut metadata) = read_messagepack::<ObjectMetadata>(&entry.path()).await {
                if metadata.fragment != fragment {
                    metadata.fragment = fragment.to_string();
                }
                match by_version.get(version_id) {
                    Some(existing) if !metadata_is_newer(&metadata, existing) => {}
                    _ => {
                        by_version.insert(version_id.to_string(), metadata);
                    }
                }
            }
        }
    }

    let mut out: Vec<ObjectMetadata> = by_version.into_values().collect();
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

async fn list_object_version_ids_internal(
    fragment: &str,
    object_key: &str,
    trashed: bool,
) -> Result<Vec<String>, AppError> {
    let route = object_route(fragment, object_key)?;
    let mut version_ids = HashSet::<String>::new();
    for root in &route.roots {
        let dir = object_base_dir(root, fragment, route.partition_id, object_key, trashed);
        if !fs::try_exists(&dir).await.unwrap_or(false) {
            continue;
        }
        let mut entries = match fs::read_dir(&dir).await {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            if !entry
                .file_type()
                .await
                .map(|ft| ft.is_file())
                .unwrap_or(false)
            {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.ends_with(INDEX_OBJECT_SUFFIX) || name == INDEX_LATEST_FILE {
                continue;
            }
            if let Some(version_id) = name.strip_suffix(INDEX_OBJECT_SUFFIX) {
                version_ids.insert(version_id.to_string());
            }
        }
    }

    let mut out: Vec<String> = version_ids.into_iter().collect();
    out.sort();
    Ok(out)
}

pub async fn list_live_object_versions_metadata(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    list_object_versions_metadata_internal(fragment, object_key, false).await
}

pub async fn list_trashed_object_versions_metadata(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    list_object_versions_metadata_internal(fragment, object_key, true).await
}

pub async fn list_live_object_version_ids(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<String>, AppError> {
    list_object_version_ids_internal(fragment, object_key, false).await
}

pub async fn list_trashed_object_version_ids(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<String>, AppError> {
    list_object_version_ids_internal(fragment, object_key, true).await
}

async fn read_latest_object_internal(
    fragment: &str,
    object_key: &str,
    trashed: bool,
) -> Result<ObjectMetadata, AppError> {
    let route = object_route(fragment, object_key)?;
    let mut latest_candidates = Vec::<(ObjectMetadata, SystemTime)>::new();
    for root in &route.roots {
        let latest_path =
            object_latest_path(root, fragment, route.partition_id, object_key, trashed);
        if !fs::try_exists(&latest_path).await.unwrap_or(false) {
            continue;
        }
        let modified = fs::metadata(&latest_path)
            .await
            .ok()
            .and_then(|meta| meta.modified().ok())
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let state = match read_messagepack::<IndexLatestState>(&latest_path).await {
            Ok(state) => state,
            Err(_) => continue,
        };
        latest_candidates.push((state.metadata, modified));
    }

    if latest_candidates.is_empty() {
        let versions = if trashed {
            list_trashed_object_versions_metadata(fragment, object_key).await?
        } else {
            list_live_object_versions_metadata(fragment, object_key).await?
        };
        if let Some(mut latest) = versions.into_iter().next() {
            if latest.fragment != fragment {
                latest.fragment = fragment.to_string();
            }
            return Ok(latest);
        }
        return Err(AppError::NotFound);
    }

    latest_candidates.sort_by(|left, right| {
        compare_version_recency(
            right.0.created_at,
            &right.0.version_id,
            left.0.created_at,
            &left.0.version_id,
        )
        .then_with(|| right.1.cmp(&left.1))
    });
    let mut latest = latest_candidates.remove(0).0;
    if latest.fragment != fragment {
        latest.fragment = fragment.to_string();
    }
    Ok(latest)
}

pub async fn read_latest_object(
    fragment: &str,
    object_key: &str,
) -> Result<ObjectMetadata, AppError> {
    read_latest_object_internal(fragment, object_key, false).await
}

pub async fn read_latest_trashed_object(
    fragment: &str,
    object_key: &str,
) -> Result<ObjectMetadata, AppError> {
    read_latest_object_internal(fragment, object_key, true).await
}

pub async fn list_fragment_trash_objects(fragment: &str) -> Result<Vec<ObjectMetadata>, AppError> {
    list_fragment_objects_internal(fragment, true).await
}

pub async fn list_fragment_live_objects(fragment: &str) -> Result<Vec<ObjectMetadata>, AppError> {
    list_fragment_objects_internal(fragment, false).await
}

pub async fn scan_fragment_live_objects_page(
    fragment: &str,
    limit: usize,
    continuation_token: Option<String>,
    include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    scan_fragment_objects_page(fragment, false, limit, continuation_token, include_internal).await
}

pub async fn scan_fragment_trash_objects_page(
    fragment: &str,
    limit: usize,
    continuation_token: Option<String>,
    include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    scan_fragment_objects_page(fragment, true, limit, continuation_token, include_internal).await
}

async fn sorted_bucket_object_hashes(bucket_dir: &Path) -> Result<Vec<String>, AppError> {
    if !fs::try_exists(bucket_dir).await.unwrap_or(false) {
        return Ok(Vec::new());
    }

    let mut hashes = Vec::new();
    let mut entries = fs::read_dir(bucket_dir)
        .await
        .map_err(|e| AppError::Io(e, format!("read dir {}", bucket_dir.display())))?;
    while let Ok(Some(entry)) = entries.next_entry().await {
        if !entry
            .file_type()
            .await
            .map(|ft| ft.is_dir())
            .unwrap_or(false)
        {
            continue;
        }
        hashes.push(entry.file_name().to_string_lossy().to_string());
    }
    hashes.sort_unstable();
    Ok(hashes)
}

async fn scan_partition_root_base(
    fragment: &str,
    partition_id: u16,
    trashed: bool,
) -> Result<Option<PathBuf>, AppError> {
    let route = route_for_partition(partition_id)?;
    for root in &route.roots {
        let base = objects_dir_for_partition(root, fragment, partition_id, trashed);
        if fs::try_exists(&base).await.unwrap_or(false) {
            return Ok(Some(base));
        }
    }
    Ok(None)
}

async fn scan_fragment_objects_page(
    fragment: &str,
    trashed: bool,
    limit: usize,
    continuation_token: Option<String>,
    include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    let limit = limit.clamp(1, 1000);
    let cursor = match continuation_token.as_deref() {
        Some(token) => Some(decode_fragment_scan_token(token).ok_or_else(|| {
            AppError::BadRequest("invalid flat-list continuation token".to_string())
        })?),
        None => None,
    };
    let start_partition_id = cursor.as_ref().map(|v| v.partition_id).unwrap_or(0);
    let start_bucket_id = cursor.as_ref().map(|v| v.bucket_id).unwrap_or(0);
    let start_after_hash = cursor
        .as_ref()
        .map(|v| v.after_hash.clone())
        .unwrap_or_default();

    let mut objects = Vec::with_capacity(limit);

    for partition_id in start_partition_id..INDEX_PARTITION_COUNT {
        let Some(base) = scan_partition_root_base(fragment, partition_id, trashed).await? else {
            continue;
        };

        let bucket_start = if partition_id == start_partition_id {
            start_bucket_id
        } else {
            0
        };

        for bucket_id in bucket_start..=u8::MAX {
            let bucket_dir = base.join(format!("{:02x}", bucket_id));
            let hashes = sorted_bucket_object_hashes(&bucket_dir).await?;
            if hashes.is_empty() {
                continue;
            }

            for object_hash in hashes {
                if partition_id == start_partition_id
                    && bucket_id == start_bucket_id
                    && !start_after_hash.is_empty()
                    && object_hash <= start_after_hash
                {
                    continue;
                }

                let latest_path = bucket_dir.join(&object_hash).join(INDEX_LATEST_FILE);
                let Some(mut metadata) =
                    read_messagepack_if_exists::<IndexLatestState>(&latest_path)
                        .await?
                        .map(|state| state.metadata)
                else {
                    continue;
                };

                if metadata.fragment != fragment {
                    metadata.fragment = fragment.to_string();
                }
                if metadata.is_delete_marker {
                    continue;
                }
                if !include_internal && is_internal_object_key(&metadata.object_key) {
                    continue;
                }

                objects.push(metadata);
                if objects.len() >= limit {
                    return Ok(ListObjectsResult {
                        objects,
                        common_prefixes: Vec::new(),
                        continuation_token: Some(encode_fragment_scan_token(&FragmentScanCursor {
                            partition_id,
                            bucket_id,
                            after_hash: object_hash,
                        })),
                    });
                }
            }
        }
    }

    Ok(ListObjectsResult {
        objects,
        common_prefixes: Vec::new(),
        continuation_token: None,
    })
}

async fn list_fragment_objects_internal(
    fragment: &str,
    trashed: bool,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let roots = ObjectPathManager::new().get_all_index_roots();
    if roots.is_empty() {
        return Ok(Vec::new());
    }

    let mut by_key = HashMap::<String, ObjectMetadata>::new();

    for partition_id in 0..INDEX_PARTITION_COUNT {
        for root in &roots {
            let base = partition_dir(root, fragment, partition_id).join(if trashed {
                INDEX_TRASH_DIR
            } else {
                INDEX_OBJECTS_DIR
            });
            if !fs::try_exists(&base).await.unwrap_or(false) {
                continue;
            }

            let mut l1 = match fs::read_dir(&base).await {
                Ok(entries) => entries,
                Err(_) => continue,
            };
            while let Ok(Some(p1)) = l1.next_entry().await {
                if !p1.file_type().await.map(|ft| ft.is_dir()).unwrap_or(false) {
                    continue;
                }

                let mut l2 = match fs::read_dir(p1.path()).await {
                    Ok(entries) => entries,
                    Err(_) => continue,
                };
                while let Ok(Some(obj_dir)) = l2.next_entry().await {
                    if !obj_dir
                        .file_type()
                        .await
                        .map(|ft| ft.is_dir())
                        .unwrap_or(false)
                    {
                        continue;
                    }

                    let latest_path = obj_dir.path().join(INDEX_LATEST_FILE);
                    let metadata = if fs::try_exists(&latest_path).await.unwrap_or(false) {
                        read_messagepack::<IndexLatestState>(&latest_path)
                            .await
                            .ok()
                            .map(|state| state.metadata)
                    } else {
                        None
                    };
                    let Some(mut metadata) = metadata else {
                        continue;
                    };
                    if metadata.fragment != fragment {
                        metadata.fragment = fragment.to_string();
                    }
                    let key = canonical_key(&metadata.object_key);
                    match by_key.get(&key) {
                        Some(existing) if !metadata_is_newer(&metadata, existing) => {}
                        _ => {
                            by_key.insert(key, metadata);
                        }
                    }
                }
            }
        }
    }

    let mut out: Vec<ObjectMetadata> = by_key.into_values().collect();
    out.sort_by(|left, right| {
        left.object_key
            .cmp(&right.object_key)
            .then(compare_version_recency(
                right.created_at,
                &right.version_id,
                left.created_at,
                &left.version_id,
            ))
    });
    Ok(out)
}

pub async fn move_object_version_to_trash_with_metadata(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<ObjectMetadata, AppError> {
    let metadata = read_object_version(fragment, object_key, version_id).await?;
    upsert_trashed_object_version(&metadata).await?;
    delete_object_version(fragment, object_key, version_id).await?;
    Ok(metadata)
}

#[allow(dead_code)]
pub async fn move_object_version_to_trash(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<(), AppError> {
    move_object_version_to_trash_with_metadata(fragment, object_key, version_id)
        .await
        .map(|_| ())
}

pub async fn restore_object_version_from_trash_with_metadata(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<ObjectMetadata, AppError> {
    let metadata = read_trashed_object_version(fragment, object_key, version_id).await?;
    upsert_object_version(&metadata).await?;
    delete_trashed_object_version(fragment, object_key, version_id).await?;
    Ok(metadata)
}

#[cfg(test)]
mod tests {
    use super::{FragmentScanCursor, decode_fragment_scan_token, encode_fragment_scan_token};

    #[test]
    fn fragment_scan_token_round_trip() {
        let token = encode_fragment_scan_token(&FragmentScanCursor {
            partition_id: 0x002a,
            bucket_id: 0x7f,
            after_hash: "abcdef0123456789".to_string(),
        });
        let parsed = decode_fragment_scan_token(&token).expect("scan token should parse");
        assert_eq!(parsed.partition_id, 0x002a);
        assert_eq!(parsed.bucket_id, 0x7f);
        assert_eq!(parsed.after_hash, "abcdef0123456789");
    }
}

pub async fn move_object_to_trash_fast(
    fragment: &str,
    object_key: &str,
) -> Result<Option<Vec<String>>, AppError> {
    let route = object_route(fragment, object_key)?;
    let mut live_dirs = Vec::<PathBuf>::new();
    let mut trash_dirs = Vec::<PathBuf>::new();
    let mut version_ids = HashSet::<String>::new();

    for root in &route.roots {
        let src = object_base_dir(root, fragment, route.partition_id, object_key, false);
        let dst = object_base_dir(root, fragment, route.partition_id, object_key, true);
        if fs::try_exists(&dst).await.unwrap_or(false) {
            return Ok(None);
        }
        if fs::try_exists(&src).await.unwrap_or(false) {
            for version_id in list_version_ids_from_object_dir(&src).await? {
                version_ids.insert(version_id);
            }
            live_dirs.push(src);
            trash_dirs.push(dst);
        }
    }

    if live_dirs.is_empty() {
        return Ok(None);
    }

    for dst in &trash_dirs {
        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| AppError::Io(e, format!("mkdir {}", parent.display())))?;
        }
    }

    for (src, dst) in live_dirs.iter().zip(trash_dirs.iter()) {
        fs::rename(src, dst).await.map_err(|e| {
            AppError::Io(
                e,
                format!("rename object dir {} -> {}", src.display(), dst.display()),
            )
        })?;
    }

    let mut out: Vec<String> = version_ids.into_iter().collect();
    out.sort_by(|left, right| compare_version_id_recency(left, right));
    Ok(Some(out))
}

pub async fn restore_object_from_trash_fast(
    fragment: &str,
    object_key: &str,
) -> Result<Option<Vec<String>>, AppError> {
    let route = object_route(fragment, object_key)?;
    let mut live_dirs = Vec::<PathBuf>::new();
    let mut trash_dirs = Vec::<PathBuf>::new();
    let mut version_ids = HashSet::<String>::new();

    for root in &route.roots {
        let src = object_base_dir(root, fragment, route.partition_id, object_key, true);
        let dst = object_base_dir(root, fragment, route.partition_id, object_key, false);
        if fs::try_exists(&dst).await.unwrap_or(false) {
            return Ok(None);
        }
        if fs::try_exists(&src).await.unwrap_or(false) {
            for version_id in list_version_ids_from_object_dir(&src).await? {
                version_ids.insert(version_id);
            }
            trash_dirs.push(src);
            live_dirs.push(dst);
        }
    }

    if trash_dirs.is_empty() {
        return Ok(None);
    }

    for dst in &live_dirs {
        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| AppError::Io(e, format!("mkdir {}", parent.display())))?;
        }
    }

    for (src, dst) in trash_dirs.iter().zip(live_dirs.iter()) {
        fs::rename(src, dst).await.map_err(|e| {
            AppError::Io(
                e,
                format!("rename object dir {} -> {}", src.display(), dst.display()),
            )
        })?;
    }

    let mut out: Vec<String> = version_ids.into_iter().collect();
    out.sort_by(|left, right| compare_version_id_recency(left, right));
    Ok(Some(out))
}

pub async fn delete_object_fast(
    fragment: &str,
    object_key: &str,
) -> Result<Option<Vec<String>>, AppError> {
    let mut version_ids = HashSet::<String>::new();

    if let Some(live_ids) = remove_object_dir_fast_internal(fragment, object_key, false).await? {
        version_ids.extend(live_ids);
    }
    if let Some(trash_ids) = remove_object_dir_fast_internal(fragment, object_key, true).await? {
        version_ids.extend(trash_ids);
    }

    if version_ids.is_empty() {
        return Ok(None);
    }

    let mut out: Vec<String> = version_ids.into_iter().collect();
    out.sort_by(|left, right| compare_version_id_recency(left, right));
    Ok(Some(out))
}

async fn recompute_latest_state(
    fragment: &str,
    object_key: &str,
    trashed: bool,
) -> Result<Option<ObjectMetadata>, AppError> {
    let versions = list_object_versions_metadata_internal(fragment, object_key, trashed).await?;
    Ok(versions.into_iter().next())
}

pub async fn delete_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<(), AppError> {
    delete_object_version_internal(fragment, object_key, version_id, false).await
}

pub async fn delete_trashed_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<(), AppError> {
    delete_object_version_internal(fragment, object_key, version_id, true).await
}

async fn delete_object_version_internal(
    fragment: &str,
    object_key: &str,
    version_id: &str,
    trashed: bool,
) -> Result<(), AppError> {
    let route = object_route(fragment, object_key)?;
    let mut removed = false;
    for root in &route.roots {
        let version_path = object_version_path(
            root,
            fragment,
            route.partition_id,
            object_key,
            version_id,
            trashed,
        );
        if fs::try_exists(&version_path).await.unwrap_or(false) {
            let _ = fs::remove_file(&version_path).await;
            removed = true;
        }
    }

    let next_latest = recompute_latest_state(fragment, object_key, trashed).await?;
    for root in &route.roots {
        let latest_path =
            object_latest_path(root, fragment, route.partition_id, object_key, trashed);
        match &next_latest {
            Some(metadata) => {
                let latest_state = IndexLatestState {
                    version_id: metadata.version_id.clone(),
                    metadata: metadata.clone(),
                };
                if let Ok(bytes) = rmp_serde::to_vec(&latest_state) {
                    let _ = write_atomic(&latest_path, &bytes).await;
                }
            }
            None => {
                let _ = fs::remove_file(&latest_path).await;
            }
        }
    }

    if !removed {
        return Err(AppError::NotFound);
    }
    Ok(())
}

pub async fn clear_fragment_trash_in_partitions(
    fragment: &str,
    object_key: Option<&str>,
) -> Result<(), AppError> {
    let roots = ObjectPathManager::new().get_all_index_roots();
    if let Some(key) = object_key {
        let route = object_route(fragment, key)?;
        for root in &route.roots {
            let dir = object_base_dir(root, fragment, route.partition_id, key, true);
            if fs::try_exists(&dir).await.unwrap_or(false) {
                let _ = fs::remove_dir_all(&dir).await;
            }
        }
        return Ok(());
    }

    for partition_id in 0..INDEX_PARTITION_COUNT {
        for root in &roots {
            let trash_dir = partition_dir(root, fragment, partition_id).join(INDEX_TRASH_DIR);
            if fs::try_exists(&trash_dir).await.unwrap_or(false) {
                let _ = fs::remove_dir_all(&trash_dir).await;
            }
        }
    }
    Ok(())
}
