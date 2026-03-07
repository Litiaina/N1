use crate::enums::app_error_enum::AppError;
use crate::n1_core::disk_manager::{
    ObjectMetadata, ObjectPathManager, PartMetadata, ReservationReceipt,
};
use crate::n1_core::upload::UploadSessionManifest;
use chrono::Utc;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
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
const RESERVATION_FILE: &str = "reservation.msgpack";

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

fn canonical_key(object_key: &str) -> String {
    ObjectPathManager::canonical_prefix(object_key)
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

pub async fn read_latest_object(
    fragment: &str,
    object_key: &str,
) -> Result<ObjectMetadata, AppError> {
    let route = object_route(fragment, object_key)?;
    let mut latest_candidates = Vec::<(ObjectMetadata, SystemTime)>::new();
    for root in &route.roots {
        let latest_path = object_latest_path(root, fragment, route.partition_id, object_key, false);
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
        let versions = list_live_object_versions_metadata(fragment, object_key).await?;
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

pub async fn list_fragment_trash_objects(fragment: &str) -> Result<Vec<ObjectMetadata>, AppError> {
    list_fragment_objects_internal(fragment, true).await
}

pub async fn list_fragment_live_objects(fragment: &str) -> Result<Vec<ObjectMetadata>, AppError> {
    list_fragment_objects_internal(fragment, false).await
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

pub async fn move_object_version_to_trash(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<(), AppError> {
    let metadata = read_object_version(fragment, object_key, version_id).await?;
    upsert_trashed_object_version(&metadata).await?;
    delete_object_version(fragment, object_key, version_id).await
}

pub async fn restore_object_version_from_trash(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<(), AppError> {
    let metadata = read_trashed_object_version(fragment, object_key, version_id).await?;
    upsert_object_version(&metadata).await?;
    delete_trashed_object_version(fragment, object_key, version_id).await
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
