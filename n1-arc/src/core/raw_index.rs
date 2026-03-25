use crate::core::blob_store::open_or_format_raw_manager_for_path;
use crate::core::disk_manager::{
    ObjectMetadata, PartMetadata, Quota, compare_version_recency, configured_index_store_disks,
};
use crate::core::fragment_metadata::FragmentMetadata;
use crate::core::index_types::{
    IndexRuntimeStats, IndexSessionState, RemovedSessionReceipt, SessionPartReceipt,
    UploadRuntimeStats,
};
use crate::core::upload::UploadSessionManifest;
use crate::enums::app_error_enum::AppError;
use block_manager::{AllocationHandle, BlockManager};
use crc32fast::hash as crc32_hash;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock as TokioRwLock, mpsc};

const RAW_INDEX_MAGIC: &[u8; 8] = b"N1ARCIDX";
const RAW_INDEX_RECORD_HEADER_BYTES: usize = 20;
const RAW_INDEX_CATALOG_BYTES: u64 = 64 * 1024 * 1024;

const RAW_INDEX_WAL_MAGIC: &[u8; 8] = b"N1ARCWAL";
const RAW_INDEX_WAL_VERSION: u32 = 1;
const RAW_INDEX_WAL_HEADER_BYTES: usize = 16;
const RAW_INDEX_WAL_RECORD_MAGIC: &[u8; 4] = b"RWL1";
const RAW_INDEX_WAL_RECORD_HEADER_BYTES: usize = 24;
const RAW_INDEX_WAL_BYTES: u64 = 32 * 1024 * 1024;
const RAW_INDEX_WAL_CHECKPOINT_INTERVAL: u64 = 256;
const RAW_INDEX_WAL_WATERMARK_BYTES: u64 = RAW_INDEX_WAL_BYTES * 3 / 4;
const RAW_INDEX_STREAM_BUFFER: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RawIndexState {
    #[serde(default)]
    sessions: HashMap<String, IndexSessionState>,
    #[serde(default)]
    session_receipts: HashMap<String, BTreeMap<u64, SessionPartReceipt>>,
    #[serde(default)]
    removed_sessions: HashMap<String, RemovedSessionReceipt>,
    #[serde(default)]
    live_versions: HashMap<String, ObjectMetadata>,
    #[serde(default)]
    trash_versions: HashMap<String, ObjectMetadata>,
    #[serde(default)]
    quotas: HashMap<String, Quota>,
    #[serde(default)]
    fragment_metadata: HashMap<String, FragmentMetadata>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
enum RawIndexKeySpace {
    Sessions = 1,
    SessionReceipts = 2,
    RemovedSessions = 3,
    LiveVersions = 4,
    TrashVersions = 5,
    Quotas = 6,
    FragmentMetadata = 7,
    Maintenance = 8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum RawWalMutation {
    OpenSession(IndexSessionState),
    UpsertSessionPart {
        fragment: String,
        version_id: String,
        part: PartMetadata,
    },
    UpsertSessionReceipt {
        fragment: String,
        version_id: String,
        receipt: SessionPartReceipt,
    },
    RemoveSession {
        fragment: String,
        version_id: String,
        receipt: RemovedSessionReceipt,
    },
    DeleteRemovedSessionReceipt {
        fragment: String,
        version_id: String,
    },
    UpsertLiveVersion(ObjectMetadata),
    UpsertTrashVersion(ObjectMetadata),
    DeleteLiveVersion {
        fragment: String,
        object_key: String,
        version_id: String,
    },
    DeleteTrashVersion {
        fragment: String,
        object_key: String,
        version_id: String,
    },
    MoveVersionToTrash(ObjectMetadata),
    RestoreVersionFromTrash(ObjectMetadata),
    ClearObjectLatestStates {
        fragment: String,
        object_key: String,
    },
    ClearFragmentTrash {
        fragment: String,
    },
    UpsertQuota {
        fragment: String,
        quota: Quota,
    },
    UpsertFragmentMetadata(FragmentMetadata),
    DeleteFragmentState {
        fragment: String,
    },
}

impl RawWalMutation {
    fn header_codes(&self) -> (RawIndexKeySpace, u16) {
        match self {
            Self::OpenSession(_) => (RawIndexKeySpace::Sessions, 1),
            Self::UpsertSessionPart { .. } => (RawIndexKeySpace::Sessions, 2),
            Self::UpsertSessionReceipt { .. } => (RawIndexKeySpace::SessionReceipts, 1),
            Self::RemoveSession { .. } => (RawIndexKeySpace::RemovedSessions, 1),
            Self::DeleteRemovedSessionReceipt { .. } => (RawIndexKeySpace::RemovedSessions, 2),
            Self::UpsertLiveVersion(_) => (RawIndexKeySpace::LiveVersions, 1),
            Self::UpsertTrashVersion(_) => (RawIndexKeySpace::TrashVersions, 1),
            Self::DeleteLiveVersion { .. } => (RawIndexKeySpace::LiveVersions, 2),
            Self::DeleteTrashVersion { .. } => (RawIndexKeySpace::TrashVersions, 2),
            Self::MoveVersionToTrash(_) => (RawIndexKeySpace::TrashVersions, 3),
            Self::RestoreVersionFromTrash(_) => (RawIndexKeySpace::LiveVersions, 3),
            Self::ClearObjectLatestStates { .. } => (RawIndexKeySpace::Maintenance, 1),
            Self::ClearFragmentTrash { .. } => (RawIndexKeySpace::Maintenance, 2),
            Self::UpsertQuota { .. } => (RawIndexKeySpace::Quotas, 1),
            Self::UpsertFragmentMetadata(_) => (RawIndexKeySpace::FragmentMetadata, 1),
            Self::DeleteFragmentState { .. } => (RawIndexKeySpace::Maintenance, 3),
        }
    }
}

struct RawIndexDevice {
    path: PathBuf,
    manager: Arc<std::sync::RwLock<BlockManager>>,
    checkpoint: AllocationHandle,
    wal: AllocationHandle,
}

struct RawIndexRuntime {
    devices: Vec<Arc<RawIndexDevice>>,
    generation: TokioRwLock<u64>,
    state: TokioRwLock<RawIndexState>,
    wal_positions: TokioRwLock<HashMap<PathBuf, u64>>,
    mutations_since_checkpoint: TokioRwLock<u64>,
}

static RAW_INDEX_RUNTIME: std::sync::LazyLock<Mutex<Option<Arc<RawIndexRuntime>>>> =
    std::sync::LazyLock::new(|| Mutex::new(None));

fn session_key(fragment: &str, version_id: &str) -> String {
    format!("{}|{}", fragment, version_id)
}

fn object_version_key(fragment: &str, object_key: &str, version_id: &str) -> String {
    format!("{}|{}|{}", fragment, object_key, version_id)
}

fn parse_checkpoint_record(buf: &[u8]) -> Result<Option<(u64, RawIndexState)>, AppError> {
    if buf.len() < RAW_INDEX_RECORD_HEADER_BYTES {
        return Ok(None);
    }
    if &buf[..8] != RAW_INDEX_MAGIC {
        return Ok(None);
    }
    let generation = u64::from_le_bytes(buf[8..16].try_into().map_err(|_| {
        AppError::InternalServerError("invalid raw index generation header".into())
    })?);
    let payload_len =
        u32::from_le_bytes(buf[16..20].try_into().map_err(|_| {
            AppError::InternalServerError("invalid raw index payload header".into())
        })?) as usize;
    if payload_len == 0 {
        return Ok(Some((generation, RawIndexState::default())));
    }
    let end = RAW_INDEX_RECORD_HEADER_BYTES
        .checked_add(payload_len)
        .ok_or_else(|| AppError::InternalServerError("raw index payload length overflow".into()))?;
    if end > buf.len() {
        return Ok(None);
    }
    let state: RawIndexState = rmp_serde::from_slice(&buf[RAW_INDEX_RECORD_HEADER_BYTES..end])?;
    Ok(Some((generation, state)))
}

fn encode_checkpoint_record(generation: u64, state: &RawIndexState) -> Result<Vec<u8>, AppError> {
    let payload = rmp_serde::to_vec(state)?;
    let max_payload =
        (RAW_INDEX_CATALOG_BYTES as usize).saturating_sub(RAW_INDEX_RECORD_HEADER_BYTES);
    if payload.len() > max_payload {
        return Err(AppError::InternalServerError(format!(
            "raw index catalog overflow: {} > {} bytes",
            payload.len(),
            max_payload,
        )));
    }
    let mut out = Vec::with_capacity(RAW_INDEX_RECORD_HEADER_BYTES + payload.len());
    out.extend_from_slice(RAW_INDEX_MAGIC);
    out.extend_from_slice(&generation.to_le_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

fn parse_wal_header(buf: &[u8]) -> bool {
    buf.len() >= RAW_INDEX_WAL_HEADER_BYTES
        && &buf[..8] == RAW_INDEX_WAL_MAGIC
        && u32::from_le_bytes(buf[8..12].try_into().unwrap_or([0u8; 4])) == RAW_INDEX_WAL_VERSION
}

fn initialize_wal_bytes(len: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; len];
    bytes[..8].copy_from_slice(RAW_INDEX_WAL_MAGIC);
    bytes[8..12].copy_from_slice(&RAW_INDEX_WAL_VERSION.to_le_bytes());
    bytes
}

fn detect_checkpoint_handle(manager: &BlockManager) -> Result<Option<AllocationHandle>, AppError> {
    for handle in manager.allocation_handles() {
        let cap = handle.capacity_bytes(manager.header().block_size) as usize;
        if cap < RAW_INDEX_RECORD_HEADER_BYTES {
            continue;
        }
        let bytes = manager.read(handle, 0, cap).map_err(|e| {
            AppError::InternalServerError(format!(
                "raw index scan allocation {} failed: {}",
                handle.allocation_id, e
            ))
        })?;
        if parse_checkpoint_record(&bytes)?.is_some() {
            return Ok(Some(handle));
        }
    }
    Ok(None)
}

fn detect_wal_handle(manager: &BlockManager) -> Result<Option<AllocationHandle>, AppError> {
    for handle in manager.allocation_handles() {
        let cap = handle.capacity_bytes(manager.header().block_size) as usize;
        if cap < RAW_INDEX_WAL_HEADER_BYTES {
            continue;
        }
        let bytes = manager.read(handle, 0, cap).map_err(|e| {
            AppError::InternalServerError(format!(
                "raw index scan wal allocation {} failed: {}",
                handle.allocation_id, e
            ))
        })?;
        if parse_wal_header(&bytes) {
            return Ok(Some(handle));
        }
    }
    Ok(None)
}

fn ensure_checkpoint_handle(manager: &mut BlockManager) -> Result<AllocationHandle, AppError> {
    if let Some(handle) = detect_checkpoint_handle(manager)? {
        return Ok(handle);
    }
    let handle = manager
        .allocate_bytes(RAW_INDEX_CATALOG_BYTES)
        .map_err(|e| {
            AppError::InternalServerError(format!("raw index allocate checkpoint failed: {}", e))
        })?;
    let zero_len = handle.capacity_bytes(manager.header().block_size) as usize;
    let zero = vec![0u8; zero_len];
    manager.write(handle, 0, &zero).map_err(|e| {
        AppError::InternalServerError(format!("raw index zero checkpoint failed: {}", e))
    })?;
    manager.flush_data().map_err(|e| {
        AppError::InternalServerError(format!(
            "raw index flush data after checkpoint allocation failed: {}",
            e
        ))
    })?;
    manager.flush_meta().map_err(|e| {
        AppError::InternalServerError(format!(
            "raw index flush meta after checkpoint allocation failed: {}",
            e
        ))
    })?;
    Ok(handle)
}

fn ensure_wal_handle(manager: &mut BlockManager) -> Result<AllocationHandle, AppError> {
    if let Some(handle) = detect_wal_handle(manager)? {
        return Ok(handle);
    }
    let handle = manager.allocate_bytes(RAW_INDEX_WAL_BYTES).map_err(|e| {
        AppError::InternalServerError(format!("raw index allocate wal failed: {}", e))
    })?;
    let bytes = initialize_wal_bytes(handle.capacity_bytes(manager.header().block_size) as usize);
    manager.write(handle, 0, &bytes).map_err(|e| {
        AppError::InternalServerError(format!("raw index initialize wal failed: {}", e))
    })?;
    manager.flush_data().map_err(|e| {
        AppError::InternalServerError(format!(
            "raw index flush data after wal allocation failed: {}",
            e
        ))
    })?;
    manager.flush_meta().map_err(|e| {
        AppError::InternalServerError(format!(
            "raw index flush meta after wal allocation failed: {}",
            e
        ))
    })?;
    Ok(handle)
}

fn apply_mutation(state: &mut RawIndexState, mutation: &RawWalMutation) -> Result<(), AppError> {
    match mutation {
        RawWalMutation::OpenSession(session) => {
            let key = session_key(&session.manifest.fragment, &session.manifest.version_id);
            state.sessions.insert(key.clone(), session.clone());
            state.session_receipts.remove(&key);
        }
        RawWalMutation::UpsertSessionPart {
            fragment,
            version_id,
            part,
        } => {
            let key = session_key(fragment, version_id);
            let session = state.sessions.get_mut(&key).ok_or(AppError::NotFound)?;
            session.parts.insert(part.part_index, part.clone());
            session.last_activity_at = chrono::Utc::now().timestamp();
        }
        RawWalMutation::UpsertSessionReceipt {
            fragment,
            version_id,
            receipt,
        } => {
            let key = session_key(fragment, version_id);
            let session = state.sessions.get_mut(&key).ok_or(AppError::NotFound)?;
            state
                .session_receipts
                .entry(key)
                .or_default()
                .insert(receipt.part_index, receipt.clone());
            session.last_activity_at = receipt.created_at;
        }
        RawWalMutation::RemoveSession {
            fragment,
            version_id,
            receipt,
        } => {
            let key = session_key(fragment, version_id);
            let _ = state.sessions.remove(&key);
            let _ = state.session_receipts.remove(&key);
            state.removed_sessions.insert(key, receipt.clone());
        }
        RawWalMutation::DeleteRemovedSessionReceipt {
            fragment,
            version_id,
        } => {
            state
                .removed_sessions
                .remove(&session_key(fragment, version_id));
        }
        RawWalMutation::UpsertLiveVersion(metadata) => {
            let key = object_version_key(
                &metadata.fragment,
                &metadata.object_key,
                &metadata.version_id,
            );
            state.live_versions.insert(key, metadata.clone());
        }
        RawWalMutation::UpsertTrashVersion(metadata) => {
            let key = object_version_key(
                &metadata.fragment,
                &metadata.object_key,
                &metadata.version_id,
            );
            state.trash_versions.insert(key, metadata.clone());
        }
        RawWalMutation::DeleteLiveVersion {
            fragment,
            object_key,
            version_id,
        } => {
            state
                .live_versions
                .remove(&object_version_key(fragment, object_key, version_id));
        }
        RawWalMutation::DeleteTrashVersion {
            fragment,
            object_key,
            version_id,
        } => {
            state
                .trash_versions
                .remove(&object_version_key(fragment, object_key, version_id));
        }
        RawWalMutation::MoveVersionToTrash(metadata) => {
            let key = object_version_key(
                &metadata.fragment,
                &metadata.object_key,
                &metadata.version_id,
            );
            state.live_versions.remove(&key);
            state.trash_versions.insert(key, metadata.clone());
        }
        RawWalMutation::RestoreVersionFromTrash(metadata) => {
            let key = object_version_key(
                &metadata.fragment,
                &metadata.object_key,
                &metadata.version_id,
            );
            state.trash_versions.remove(&key);
            state.live_versions.insert(key, metadata.clone());
        }
        RawWalMutation::ClearObjectLatestStates {
            fragment,
            object_key,
        } => {
            state
                .live_versions
                .retain(|_, meta| !(meta.fragment == *fragment && meta.object_key == *object_key));
            state
                .trash_versions
                .retain(|_, meta| !(meta.fragment == *fragment && meta.object_key == *object_key));
        }
        RawWalMutation::ClearFragmentTrash { fragment } => {
            state
                .trash_versions
                .retain(|_, meta| meta.fragment != *fragment);
        }
        RawWalMutation::UpsertQuota { fragment, quota } => {
            state.quotas.insert(fragment.clone(), quota.clone());
        }
        RawWalMutation::UpsertFragmentMetadata(meta) => {
            state
                .fragment_metadata
                .insert(meta.fragment.clone(), meta.clone());
        }
        RawWalMutation::DeleteFragmentState { fragment } => {
            state.fragment_metadata.remove(fragment);
            state.quotas.remove(fragment);
            state
                .sessions
                .retain(|_, session| session.manifest.fragment != *fragment);
            state
                .session_receipts
                .retain(|key, _| !key.starts_with(&format!("{fragment}|")));
            state
                .removed_sessions
                .retain(|_, receipt| receipt.state.manifest.fragment != *fragment);
            state
                .live_versions
                .retain(|_, meta| meta.fragment != *fragment);
            state
                .trash_versions
                .retain(|_, meta| meta.fragment != *fragment);
        }
    }

    Ok(())
}

fn hydrate_session_parts_from_receipts(
    session: &mut IndexSessionState,
    receipts: Option<&BTreeMap<u64, SessionPartReceipt>>,
) {
    if !session.parts.is_empty() {
        return;
    }
    let Some(receipts) = receipts else {
        return;
    };

    for (part_index, receipt) in receipts {
        session.parts.insert(
            *part_index,
            PartMetadata {
                part_index: *part_index,
                size_bytes: receipt.size_bytes,
                created_at: receipt.created_at,
                shards: Vec::new(),
                checksum: receipt.checksum.map(|value| format!("{:08x}", value)),
            },
        );
    }
}

fn encode_wal_record(generation: u64, mutation: &RawWalMutation) -> Result<Vec<u8>, AppError> {
    let (keyspace, opcode) = mutation.header_codes();
    let payload = rmp_serde::to_vec(mutation)?;
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| AppError::InternalServerError("raw index wal payload too large".into()))?;
    let checksum = crc32_hash(&payload);
    let mut out = Vec::with_capacity(RAW_INDEX_WAL_RECORD_HEADER_BYTES + payload.len());
    out.extend_from_slice(RAW_INDEX_WAL_RECORD_MAGIC);
    out.extend_from_slice(&generation.to_le_bytes());
    out.extend_from_slice(&(keyspace as u16).to_le_bytes());
    out.extend_from_slice(&opcode.to_le_bytes());
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.extend_from_slice(&checksum.to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

fn decode_wal_records(
    bytes: &[u8],
    checkpoint_generation: u64,
    state: &mut RawIndexState,
) -> Result<(u64, u64), AppError> {
    if !parse_wal_header(bytes) {
        return Ok((checkpoint_generation, RAW_INDEX_WAL_HEADER_BYTES as u64));
    }

    let mut offset = RAW_INDEX_WAL_HEADER_BYTES;
    let mut last_generation = checkpoint_generation;

    while offset + RAW_INDEX_WAL_RECORD_HEADER_BYTES <= bytes.len() {
        let header = &bytes[offset..offset + RAW_INDEX_WAL_RECORD_HEADER_BYTES];
        if &header[..4] != RAW_INDEX_WAL_RECORD_MAGIC {
            break;
        }

        let generation = u64::from_le_bytes(header[4..12].try_into().unwrap_or([0u8; 8]));
        let payload_len =
            u32::from_le_bytes(header[16..20].try_into().unwrap_or([0u8; 4])) as usize;
        let checksum = u32::from_le_bytes(header[20..24].try_into().unwrap_or([0u8; 4]));
        let next = offset
            .checked_add(RAW_INDEX_WAL_RECORD_HEADER_BYTES)
            .and_then(|value| value.checked_add(payload_len))
            .ok_or_else(|| AppError::InternalServerError("raw index wal record overflow".into()))?;
        if next > bytes.len() {
            break;
        }

        let payload = &bytes[offset + RAW_INDEX_WAL_RECORD_HEADER_BYTES..next];
        if crc32_hash(payload) != checksum {
            break;
        }

        let mutation: RawWalMutation = rmp_serde::from_slice(payload)?;
        if generation > checkpoint_generation {
            apply_mutation(state, &mutation)?;
        }
        last_generation = last_generation.max(generation);
        offset = next;
    }

    Ok((last_generation, offset as u64))
}

async fn load_device_state(
    device: &Arc<RawIndexDevice>,
) -> Result<(u64, RawIndexState, u64), AppError> {
    let manager = device.manager.read().map_err(|_| {
        AppError::InternalServerError("raw index manager read lock poisoned".into())
    })?;

    let checkpoint_cap = device
        .checkpoint
        .capacity_bytes(manager.header().block_size) as usize;
    let checkpoint_bytes = manager
        .read(device.checkpoint, 0, checkpoint_cap)
        .map_err(|e| {
            AppError::InternalServerError(format!(
                "raw index read checkpoint {} failed: {}",
                device.path.display(),
                e
            ))
        })?;
    let (checkpoint_generation, mut state) =
        parse_checkpoint_record(&checkpoint_bytes)?.unwrap_or((0, RawIndexState::default()));

    let wal_cap = device.wal.capacity_bytes(manager.header().block_size) as usize;
    let wal_bytes = manager.read(device.wal, 0, wal_cap).map_err(|e| {
        AppError::InternalServerError(format!(
            "raw index read wal {} failed: {}",
            device.path.display(),
            e
        ))
    })?;
    let (last_generation, next_offset) =
        decode_wal_records(&wal_bytes, checkpoint_generation, &mut state)?;

    Ok((last_generation, state, next_offset))
}

async fn write_checkpoint_to_devices(
    devices: &[Arc<RawIndexDevice>],
    generation: u64,
    state: &RawIndexState,
) -> Result<HashMap<PathBuf, u64>, AppError> {
    let bytes = encode_checkpoint_record(generation, state)?;
    let mut wal_positions = HashMap::new();
    let mut success = 0usize;

    for device in devices {
        let mut manager = device.manager.write().map_err(|_| {
            AppError::InternalServerError("raw index manager write lock poisoned".into())
        })?;
        manager.write(device.checkpoint, 0, &bytes).map_err(|e| {
            AppError::InternalServerError(format!(
                "raw index write checkpoint {} failed: {}",
                device.path.display(),
                e
            ))
        })?;

        let wal_len = device.wal.capacity_bytes(manager.header().block_size) as usize;
        let wal_bytes = initialize_wal_bytes(wal_len);
        manager.write(device.wal, 0, &wal_bytes).map_err(|e| {
            AppError::InternalServerError(format!(
                "raw index reset wal {} failed: {}",
                device.path.display(),
                e
            ))
        })?;
        manager.flush_data().map_err(|e| {
            AppError::InternalServerError(format!(
                "raw index checkpoint flush data {} failed: {}",
                device.path.display(),
                e
            ))
        })?;
        manager.flush_meta().map_err(|e| {
            AppError::InternalServerError(format!(
                "raw index checkpoint flush meta {} failed: {}",
                device.path.display(),
                e
            ))
        })?;

        wal_positions.insert(device.path.clone(), RAW_INDEX_WAL_HEADER_BYTES as u64);
        success += 1;
    }

    if success == 0 {
        return Err(AppError::InternalServerError(
            "failed to persist raw index checkpoint on any device".into(),
        ));
    }

    Ok(wal_positions)
}

async fn checkpoint_runtime(
    rt: &Arc<RawIndexRuntime>,
    generation: u64,
    state: &RawIndexState,
) -> Result<(), AppError> {
    let wal_positions = write_checkpoint_to_devices(&rt.devices, generation, state).await?;
    *rt.wal_positions.write().await = wal_positions;
    *rt.mutations_since_checkpoint.write().await = 0;
    Ok(())
}

async fn wal_should_checkpoint(rt: &Arc<RawIndexRuntime>, next_record_len: usize) -> bool {
    if *rt.mutations_since_checkpoint.read().await >= RAW_INDEX_WAL_CHECKPOINT_INTERVAL {
        return true;
    }

    let positions = rt.wal_positions.read().await;
    positions.values().any(|offset| {
        *offset >= RAW_INDEX_WAL_WATERMARK_BYTES
            || offset.saturating_add(next_record_len as u64) >= RAW_INDEX_WAL_BYTES
    })
}

async fn append_wal_record(
    rt: &Arc<RawIndexRuntime>,
    generation: u64,
    mutation: &RawWalMutation,
) -> Result<(), AppError> {
    let record = encode_wal_record(generation, mutation)?;
    let mut positions = rt.wal_positions.write().await;
    let mut success = 0usize;

    for device in &rt.devices {
        let start = positions
            .get(&device.path)
            .copied()
            .unwrap_or(RAW_INDEX_WAL_HEADER_BYTES as u64);
        if start.saturating_add(record.len() as u64) >= RAW_INDEX_WAL_BYTES {
            continue;
        }

        let manager = device.manager.write().map_err(|_| {
            AppError::InternalServerError("raw index manager write lock poisoned".into())
        })?;
        if manager.write(device.wal, start, &record).is_err() {
            continue;
        }
        if manager.flush_data().is_err() {
            continue;
        }
        positions.insert(device.path.clone(), start + record.len() as u64);
        success += 1;
    }

    if success == 0 {
        return Err(AppError::InternalServerError(
            "failed to append raw index wal record on any device".into(),
        ));
    }

    Ok(())
}

async fn runtime() -> Result<Arc<RawIndexRuntime>, AppError> {
    let mut guard = RAW_INDEX_RUNTIME.lock().await;
    if let Some(rt) = guard.as_ref() {
        return Ok(Arc::clone(rt));
    }

    let paths = configured_index_store_disks()?;
    if paths.is_empty() {
        return Err(AppError::InternalServerError(
            "raw index backend requires non-empty index_store_disks".into(),
        ));
    }

    let mut seen = HashSet::<PathBuf>::new();
    let mut devices = Vec::with_capacity(paths.len());
    for raw_path in paths {
        let canonical =
            std::fs::canonicalize(&raw_path).unwrap_or_else(|_| PathBuf::from(&raw_path));
        if !seen.insert(canonical.clone()) {
            continue;
        }

        let manager = open_or_format_raw_manager_for_path(Path::new(&raw_path))?;
        let (checkpoint, wal) = {
            let mut mgr = manager.write().map_err(|_| {
                AppError::InternalServerError("raw index manager write lock poisoned".into())
            })?;
            let checkpoint = ensure_checkpoint_handle(&mut mgr)?;
            let wal = ensure_wal_handle(&mut mgr)?;
            (checkpoint, wal)
        };

        devices.push(Arc::new(RawIndexDevice {
            path: canonical,
            manager,
            checkpoint,
            wal,
        }));
    }

    let mut best_generation = 0u64;
    let mut best_state = RawIndexState::default();
    let mut best_wal_positions = HashMap::<PathBuf, u64>::new();

    for device in &devices {
        let (generation, state, wal_next_offset) = load_device_state(device).await?;
        best_wal_positions.insert(device.path.clone(), wal_next_offset);
        if generation >= best_generation {
            best_generation = generation;
            best_state = state;
        }
    }

    let generation = best_generation.max(1);
    let wal_positions = write_checkpoint_to_devices(&devices, generation, &best_state).await?;

    let rt = Arc::new(RawIndexRuntime {
        devices,
        generation: TokioRwLock::new(generation),
        state: TokioRwLock::new(best_state),
        wal_positions: TokioRwLock::new(if wal_positions.is_empty() {
            best_wal_positions
        } else {
            wal_positions
        }),
        mutations_since_checkpoint: TokioRwLock::new(0),
    });
    *guard = Some(Arc::clone(&rt));
    Ok(rt)
}

pub async fn ensure_initialized() -> Result<(), AppError> {
    let _ = runtime().await?;
    Ok(())
}

async fn persist_prepared_mutation<F, T>(prepare: F) -> Result<T, AppError>
where
    F: FnOnce(&RawIndexState) -> Result<(T, Option<RawWalMutation>), AppError>,
{
    let rt = runtime().await?;
    let mut state_guard = rt.state.write().await;
    let (out, mutation) = prepare(&state_guard)?;
    let Some(mutation) = mutation else {
        return Ok(out);
    };

    if wal_should_checkpoint(&rt, encode_wal_record(0, &mutation)?.len()).await {
        let generation = *rt.generation.read().await;
        let snapshot = state_guard.clone();
        checkpoint_runtime(&rt, generation, &snapshot).await?;
    }

    let next_generation = {
        let generation = rt.generation.read().await;
        generation.saturating_add(1)
    };
    append_wal_record(&rt, next_generation, &mutation).await?;
    apply_mutation(&mut state_guard, &mutation)?;
    *rt.generation.write().await = next_generation;

    let mut ops = rt.mutations_since_checkpoint.write().await;
    *ops = ops.saturating_add(1);
    let should_checkpoint = *ops >= RAW_INDEX_WAL_CHECKPOINT_INTERVAL;
    drop(ops);

    if should_checkpoint {
        let snapshot = state_guard.clone();
        checkpoint_runtime(&rt, next_generation, &snapshot).await?;
    }

    Ok(out)
}

pub async fn open_session(
    manifest: &UploadSessionManifest,
    reservation: Option<crate::core::disk_manager::ReservationReceipt>,
) -> Result<(), AppError> {
    let session = IndexSessionState {
        manifest: manifest.clone(),
        reservation,
        parts: BTreeMap::new(),
        last_activity_at: chrono::Utc::now().timestamp(),
        finalized_at: None,
        aborted_at: None,
    };
    persist_prepared_mutation(|_| Ok(((), Some(RawWalMutation::OpenSession(session))))).await
}

#[allow(dead_code)]
pub async fn commit_session_part(
    fragment: &str,
    version_id: &str,
    part: &crate::core::disk_manager::PartMetadata,
) -> Result<(), AppError> {
    let fragment = fragment.to_string();
    let version_id = version_id.to_string();
    let part = part.clone();
    persist_prepared_mutation(|state| {
        if !state
            .sessions
            .contains_key(&session_key(&fragment, &version_id))
        {
            return Err(AppError::NotFound);
        }
        Ok((
            (),
            Some(RawWalMutation::UpsertSessionPart {
                fragment,
                version_id,
                part,
            }),
        ))
    })
    .await
}

pub async fn commit_session_part_receipt(
    fragment: &str,
    version_id: &str,
    receipt: &SessionPartReceipt,
) -> Result<(), AppError> {
    let fragment = fragment.to_string();
    let version_id = version_id.to_string();
    let receipt = receipt.clone();
    persist_prepared_mutation(|state| {
        if !state
            .sessions
            .contains_key(&session_key(&fragment, &version_id))
        {
            return Err(AppError::NotFound);
        }
        Ok((
            (),
            Some(RawWalMutation::UpsertSessionReceipt {
                fragment,
                version_id,
                receipt,
            }),
        ))
    })
    .await
}

pub async fn load_session(
    fragment: &str,
    version_id: &str,
) -> Result<Option<IndexSessionState>, AppError> {
    let rt = runtime().await?;
    let state = rt.state.read().await;
    let key = session_key(fragment, version_id);
    let mut session = state.sessions.get(&key).cloned();
    if let Some(session) = session.as_mut() {
        hydrate_session_parts_from_receipts(session, state.session_receipts.get(&key));
    }
    Ok(session)
}

pub async fn load_session_manifest(
    fragment: &str,
    version_id: &str,
) -> Result<Option<UploadSessionManifest>, AppError> {
    let rt = runtime().await?;
    let state = rt.state.read().await;
    Ok(state
        .sessions
        .get(&session_key(fragment, version_id))
        .map(|session| session.manifest.clone()))
}

pub async fn read_session_part(
    fragment: &str,
    version_id: &str,
    part_index: u64,
) -> Result<Option<crate::core::disk_manager::PartMetadata>, AppError> {
    let rt = runtime().await?;
    let state = rt.state.read().await;
    Ok(state
        .sessions
        .get(&session_key(fragment, version_id))
        .and_then(|session| session.parts.get(&part_index))
        .cloned())
}

pub async fn read_session_part_receipt(
    fragment: &str,
    version_id: &str,
    part_index: u64,
) -> Result<Option<SessionPartReceipt>, AppError> {
    let rt = runtime().await?;
    let state = rt.state.read().await;
    Ok(state
        .session_receipts
        .get(&session_key(fragment, version_id))
        .and_then(|parts| parts.get(&part_index))
        .cloned())
}

pub async fn load_session_part_receipts(
    fragment: &str,
    version_id: &str,
    manifest: &UploadSessionManifest,
) -> Result<Option<Vec<Option<SessionPartReceipt>>>, AppError> {
    let rt = runtime().await?;
    let state = rt.state.read().await;
    let Some(parts) = state
        .session_receipts
        .get(&session_key(fragment, version_id))
    else {
        return Ok(None);
    };
    let mut receipts = vec![None; manifest.expected_parts as usize];
    for (part_index, receipt) in parts {
        if let Some(slot) = receipts.get_mut(*part_index as usize) {
            *slot = Some(receipt.clone());
        }
    }
    Ok(Some(receipts))
}

pub async fn remove_session(
    fragment: &str,
    version_id: &str,
    reason: &str,
) -> Result<(), AppError> {
    let fragment = fragment.to_string();
    let version_id = version_id.to_string();
    let reason = reason.to_string();
    persist_prepared_mutation(|state| {
        let key = session_key(&fragment, &version_id);
        let session = state
            .sessions
            .get(&key)
            .cloned()
            .ok_or(AppError::NotFound)?;
        let session_part_receipts = state
            .session_receipts
            .get(&key)
            .cloned()
            .unwrap_or_default();
        let receipt = RemovedSessionReceipt {
            state: session,
            session_part_receipts,
            removed_at: chrono::Utc::now().timestamp(),
            reason,
        };
        Ok((
            (),
            Some(RawWalMutation::RemoveSession {
                fragment,
                version_id,
                receipt,
            }),
        ))
    })
    .await
}

pub async fn list_active_sessions(fragment: &str) -> Result<Vec<IndexSessionState>, AppError> {
    let rt = runtime().await?;
    let state = rt.state.read().await;
    let mut sessions = Vec::new();
    for (key, session) in &state.sessions {
        if session.manifest.fragment != fragment {
            continue;
        }
        let mut session = session.clone();
        hydrate_session_parts_from_receipts(&mut session, state.session_receipts.get(key));
        sessions.push(session);
    }
    Ok(sessions)
}

pub async fn read_removed_session_receipt(
    fragment: &str,
    version_id: &str,
) -> Result<Option<RemovedSessionReceipt>, AppError> {
    let rt = runtime().await?;
    let state = rt.state.read().await;
    let mut receipt = state
        .removed_sessions
        .get(&session_key(fragment, version_id))
        .cloned();
    if let Some(receipt) = receipt.as_mut() {
        hydrate_session_parts_from_receipts(
            &mut receipt.state,
            Some(&receipt.session_part_receipts),
        );
    }
    Ok(receipt)
}

pub async fn list_removed_session_receipts(
    fragment: &str,
) -> Result<Vec<(String, RemovedSessionReceipt)>, AppError> {
    let rt = runtime().await?;
    let state = rt.state.read().await;
    let mut out = state
        .removed_sessions
        .iter()
        .filter_map(|(key, receipt)| {
            if receipt.state.manifest.fragment != fragment {
                return None;
            }
            let mut receipt = receipt.clone();
            hydrate_session_parts_from_receipts(
                &mut receipt.state,
                Some(&receipt.session_part_receipts),
            );
            Some((key.clone(), receipt))
        })
        .collect::<Vec<_>>();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

pub async fn delete_removed_session_receipt(
    fragment: &str,
    version_id: &str,
) -> Result<(), AppError> {
    let fragment = fragment.to_string();
    let version_id = version_id.to_string();
    persist_prepared_mutation(|state| {
        let key = session_key(&fragment, &version_id);
        if !state.removed_sessions.contains_key(&key) {
            return Ok(((), None));
        }
        Ok((
            (),
            Some(RawWalMutation::DeleteRemovedSessionReceipt {
                fragment,
                version_id,
            }),
        ))
    })
    .await
}

fn choose_latest_for_key<'a>(
    versions: impl Iterator<Item = &'a ObjectMetadata>,
) -> Option<ObjectMetadata> {
    let mut latest: Option<ObjectMetadata> = None;
    for item in versions {
        match latest.as_ref() {
            None => latest = Some(item.clone()),
            Some(cur) => {
                if compare_version_recency(
                    item.created_at,
                    &item.version_id,
                    cur.created_at,
                    &cur.version_id,
                ) == std::cmp::Ordering::Greater
                {
                    latest = Some(item.clone());
                }
            }
        }
    }
    latest
}

pub async fn publish_object(metadata: &ObjectMetadata) -> Result<(), AppError> {
    upsert_object_version(metadata, false).await
}

pub async fn upsert_object_version(
    metadata: &ObjectMetadata,
    trashed: bool,
) -> Result<(), AppError> {
    let metadata = metadata.clone();
    persist_prepared_mutation(|_| {
        let mutation = if trashed {
            RawWalMutation::UpsertTrashVersion(metadata)
        } else {
            RawWalMutation::UpsertLiveVersion(metadata)
        };
        Ok(((), Some(mutation)))
    })
    .await
}

pub async fn read_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
    trashed: bool,
) -> Result<ObjectMetadata, AppError> {
    let rt = runtime().await?;
    let state = rt.state.read().await;
    let key = object_version_key(fragment, object_key, version_id);
    let found = if trashed {
        state.trash_versions.get(&key)
    } else {
        state.live_versions.get(&key)
    };
    found.cloned().ok_or(AppError::NotFound)
}

pub async fn read_latest_object(
    fragment: &str,
    object_key: &str,
) -> Result<ObjectMetadata, AppError> {
    let rt = runtime().await?;
    let state = rt.state.read().await;
    choose_latest_for_key(
        state
            .live_versions
            .values()
            .filter(|m| m.fragment == fragment && m.object_key == object_key),
    )
    .ok_or(AppError::NotFound)
}

async fn list_latest_objects_internal(
    fragment: &str,
    trashed: bool,
    prefix: Option<&str>,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let rt = runtime().await?;
    let state = rt.state.read().await;
    let source = if trashed {
        &state.trash_versions
    } else {
        &state.live_versions
    };
    let mut by_object: HashMap<&str, Vec<&ObjectMetadata>> = HashMap::new();
    for meta in source.values() {
        if meta.fragment != fragment {
            continue;
        }
        if let Some(pfx) = prefix {
            if !meta.object_key.starts_with(pfx) {
                continue;
            }
        }
        by_object.entry(&meta.object_key).or_default().push(meta);
    }
    let mut out: Vec<ObjectMetadata> = by_object
        .into_values()
        .filter_map(|items| choose_latest_for_key(items.into_iter()))
        .collect();
    out.sort_by(|a, b| a.object_key.cmp(&b.object_key));
    Ok(out)
}

pub async fn count_fragment_latest_objects(fragment: &str, trashed: bool) -> Result<u64, AppError> {
    Ok(list_latest_objects_internal(fragment, trashed, None)
        .await?
        .len() as u64)
}

pub async fn list_fragment_latest_objects_page(
    fragment: &str,
    trashed: bool,
    prefix: Option<&str>,
    start_after: Option<(&str, &str)>,
    limit: usize,
) -> Result<(Vec<ObjectMetadata>, Option<(String, String)>), AppError> {
    let rt = runtime().await?;
    let state = rt.state.read().await;
    let source = if trashed {
        &state.trash_versions
    } else {
        &state.live_versions
    };
    let mut latest = BTreeMap::<String, ObjectMetadata>::new();

    for meta in source.values() {
        if meta.fragment != fragment {
            continue;
        }
        if let Some(pfx) = prefix {
            if !meta.object_key.starts_with(pfx) {
                continue;
            }
        }
        match latest.get(&meta.object_key) {
            Some(current)
                if compare_version_recency(
                    current.created_at,
                    &current.version_id,
                    meta.created_at,
                    &meta.version_id,
                )
                .is_ge() => {}
            _ => {
                latest.insert(meta.object_key.clone(), meta.clone());
            }
        }
    }

    let capped_limit = limit.max(1);
    let mut page = Vec::with_capacity(capped_limit);
    for (object_key, meta) in latest {
        if let Some((start_key, start_version)) = start_after {
            if (object_key.as_str(), meta.version_id.as_str()) <= (start_key, start_version) {
                continue;
            }
        }
        page.push(meta);
        if page.len() >= capped_limit {
            break;
        }
    }
    let next = page
        .last()
        .map(|m| (m.object_key.clone(), m.version_id.clone()));
    Ok((page, next))
}

pub async fn list_fragment_objects(
    fragment: &str,
    prefix: Option<&str>,
    trashed: bool,
) -> Result<Vec<ObjectMetadata>, AppError> {
    list_latest_objects_internal(fragment, trashed, prefix).await
}

pub async fn list_object_versions(
    fragment: &str,
    object_key: &str,
    trashed: bool,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let rt = runtime().await?;
    let state = rt.state.read().await;
    let source = if trashed {
        &state.trash_versions
    } else {
        &state.live_versions
    };
    let mut out: Vec<ObjectMetadata> = source
        .values()
        .filter(|m| m.fragment == fragment && m.object_key == object_key)
        .cloned()
        .collect();
    out.sort_by(|a, b| {
        compare_version_recency(b.created_at, &b.version_id, a.created_at, &a.version_id)
    });
    Ok(out)
}

pub async fn delete_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
    trashed: bool,
) -> Result<bool, AppError> {
    let fragment = fragment.to_string();
    let object_key = object_key.to_string();
    let version_id = version_id.to_string();
    persist_prepared_mutation(|state| {
        let key = object_version_key(&fragment, &object_key, &version_id);
        let exists = if trashed {
            state.trash_versions.contains_key(&key)
        } else {
            state.live_versions.contains_key(&key)
        };
        if !exists {
            return Ok((false, None));
        }
        let mutation = if trashed {
            RawWalMutation::DeleteTrashVersion {
                fragment,
                object_key,
                version_id,
            }
        } else {
            RawWalMutation::DeleteLiveVersion {
                fragment,
                object_key,
                version_id,
            }
        };
        Ok((true, Some(mutation)))
    })
    .await
}

pub async fn move_object_version_to_trash(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let fragment = fragment.to_string();
    let object_key = object_key.to_string();
    let version_id = version_id.to_string();
    persist_prepared_mutation(|state| {
        let key = object_version_key(&fragment, &object_key, &version_id);
        let Some(meta) = state.live_versions.get(&key).cloned() else {
            return Ok((None, None));
        };
        Ok((
            Some(meta.clone()),
            Some(RawWalMutation::MoveVersionToTrash(meta)),
        ))
    })
    .await
}

pub async fn restore_object_version_from_trash(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let fragment = fragment.to_string();
    let object_key = object_key.to_string();
    let version_id = version_id.to_string();
    persist_prepared_mutation(|state| {
        let key = object_version_key(&fragment, &object_key, &version_id);
        let Some(meta) = state.trash_versions.get(&key).cloned() else {
            return Ok((None, None));
        };
        Ok((
            Some(meta.clone()),
            Some(RawWalMutation::RestoreVersionFromTrash(meta)),
        ))
    })
    .await
}

pub async fn clear_object_latest_states(fragment: &str, object_key: &str) -> Result<(), AppError> {
    let fragment = fragment.to_string();
    let object_key = object_key.to_string();
    persist_prepared_mutation(|state| {
        let exists = state
            .live_versions
            .values()
            .any(|meta| meta.fragment == fragment && meta.object_key == object_key)
            || state
                .trash_versions
                .values()
                .any(|meta| meta.fragment == fragment && meta.object_key == object_key);
        if !exists {
            return Ok(((), None));
        }
        Ok((
            (),
            Some(RawWalMutation::ClearObjectLatestStates {
                fragment,
                object_key,
            }),
        ))
    })
    .await
}

pub async fn clear_fragment_trash_in_partitions(
    fragment: &str,
    _touched_partitions: &BTreeSet<u16>,
) -> Result<usize, AppError> {
    let fragment = fragment.to_string();
    persist_prepared_mutation(|state| {
        let removed = state
            .trash_versions
            .values()
            .filter(|meta| meta.fragment == fragment)
            .count();
        if removed == 0 {
            return Ok((0, None));
        }
        Ok((
            removed,
            Some(RawWalMutation::ClearFragmentTrash { fragment }),
        ))
    })
    .await
}

pub async fn stream_fragment_object_versions(
    fragment: String,
    trashed: bool,
) -> mpsc::Receiver<Result<ObjectMetadata, AppError>> {
    let (tx, rx) = mpsc::channel(RAW_INDEX_STREAM_BUFFER);
    tokio::spawn(async move {
        match runtime().await {
            Ok(rt) => {
                let state = rt.state.read().await;
                let source = if trashed {
                    &state.trash_versions
                } else {
                    &state.live_versions
                };
                let mut out: Vec<ObjectMetadata> = source
                    .values()
                    .filter(|m| m.fragment == fragment)
                    .cloned()
                    .collect();
                out.sort_by(|a, b| a.object_key.cmp(&b.object_key));
                for meta in out {
                    if tx.send(Ok(meta)).await.is_err() {
                        break;
                    }
                }
            }
            Err(e) => {
                let _ = tx.send(Err(e)).await;
            }
        }
    });
    rx
}

pub async fn collect_runtime_upload_stats() -> UploadRuntimeStats {
    match runtime().await {
        Ok(rt) => {
            let state = rt.state.read().await;
            let mut active_fragments = BTreeSet::new();
            let mut committed_parts = 0u64;
            let mut committed_bytes = 0u64;
            let mut reserved_bytes = 0u64;
            let mut oldest_created_at: Option<i64> = None;

            for (key, session) in &state.sessions {
                active_fragments.insert(session.manifest.fragment.clone());
                reserved_bytes = reserved_bytes
                    .saturating_add(session.reservation.as_ref().map(|r| r.amount).unwrap_or(0));
                oldest_created_at = match oldest_created_at {
                    Some(current) => Some(current.min(session.manifest.created_at)),
                    None => Some(session.manifest.created_at),
                };
                if !session.parts.is_empty() {
                    committed_parts = committed_parts.saturating_add(session.parts.len() as u64);
                    committed_bytes = committed_bytes.saturating_add(
                        session
                            .parts
                            .values()
                            .map(|part| part.size_bytes)
                            .sum::<u64>(),
                    );
                    continue;
                }
                if let Some(receipts) = state.session_receipts.get(key) {
                    committed_parts = committed_parts.saturating_add(receipts.len() as u64);
                    committed_bytes = committed_bytes.saturating_add(
                        receipts
                            .values()
                            .map(|receipt| receipt.size_bytes)
                            .sum::<u64>(),
                    );
                }
            }

            let oldest_session_age_seconds = oldest_created_at.and_then(|created_at| {
                let now = chrono::Utc::now().timestamp();
                (now >= created_at).then_some((now - created_at) as u64)
            });

            UploadRuntimeStats {
                active_fragments: active_fragments.len() as u64,
                active_sessions: state.sessions.len() as u64,
                reserved_bytes,
                committed_parts,
                committed_bytes,
                oldest_session_age_seconds,
            }
        }
        Err(_) => UploadRuntimeStats::default(),
    }
}

pub async fn collect_index_runtime_stats() -> IndexRuntimeStats {
    match runtime().await {
        Ok(rt) => IndexRuntimeStats {
            loaded_partition_runtimes: 1,
            wal_appenders: rt.devices.len() as u64,
            prefix_projection_fragments_pending: 0,
        },
        Err(_) => IndexRuntimeStats::default(),
    }
}

pub async fn is_prefix_projection_pending(_fragment: &str) -> bool {
    false
}

pub async fn list_fragments() -> Result<Vec<String>, AppError> {
    let rt = runtime().await?;
    let state = rt.state.read().await;
    let mut fragments = BTreeSet::new();
    fragments.extend(state.fragment_metadata.keys().cloned());
    fragments.extend(state.quotas.keys().cloned());
    fragments.extend(state.sessions.values().map(|s| s.manifest.fragment.clone()));
    fragments.extend(state.live_versions.values().map(|m| m.fragment.clone()));
    fragments.extend(state.trash_versions.values().map(|m| m.fragment.clone()));
    Ok(fragments.into_iter().collect())
}

pub async fn fragment_exists(fragment: &str) -> Result<bool, AppError> {
    let rt = runtime().await?;
    let state = rt.state.read().await;
    Ok(state.fragment_metadata.contains_key(fragment)
        || state.quotas.contains_key(fragment)
        || state
            .sessions
            .values()
            .any(|s| s.manifest.fragment == fragment)
        || state.live_versions.values().any(|m| m.fragment == fragment)
        || state
            .trash_versions
            .values()
            .any(|m| m.fragment == fragment))
}

pub async fn read_quota(fragment: &str) -> Result<Option<Quota>, AppError> {
    let rt = runtime().await?;
    let state = rt.state.read().await;
    Ok(state.quotas.get(fragment).cloned())
}

pub async fn upsert_quota(fragment: &str, quota: &Quota) -> Result<(), AppError> {
    let fragment = fragment.to_string();
    let quota = quota.clone();
    persist_prepared_mutation(|_| Ok(((), Some(RawWalMutation::UpsertQuota { fragment, quota }))))
        .await
}

pub async fn read_fragment_metadata(fragment: &str) -> Result<Option<FragmentMetadata>, AppError> {
    let rt = runtime().await?;
    let state = rt.state.read().await;
    Ok(state.fragment_metadata.get(fragment).cloned())
}

pub async fn upsert_fragment_metadata(meta: &FragmentMetadata) -> Result<(), AppError> {
    let meta = meta.clone();
    persist_prepared_mutation(|_| Ok(((), Some(RawWalMutation::UpsertFragmentMetadata(meta)))))
        .await
}

pub async fn delete_fragment_state(fragment: &str) -> Result<(), AppError> {
    let fragment = fragment.to_string();
    persist_prepared_mutation(|state| {
        let exists = state.fragment_metadata.contains_key(&fragment)
            || state.quotas.contains_key(&fragment)
            || state
                .sessions
                .values()
                .any(|s| s.manifest.fragment == fragment)
            || state
                .removed_sessions
                .values()
                .any(|r| r.state.manifest.fragment == fragment)
            || state.live_versions.values().any(|m| m.fragment == fragment)
            || state
                .trash_versions
                .values()
                .any(|m| m.fragment == fragment);
        if !exists {
            return Ok(((), None));
        }
        Ok(((), Some(RawWalMutation::DeleteFragmentState { fragment })))
    })
    .await
}

#[cfg(test)]
pub async fn clear_runtime_caches_for_tests() {
    let mut guard = RAW_INDEX_RUNTIME.lock().await;
    *guard = None;
}

#[cfg(test)]
mod tests {
    use super::*;
    use block_manager::{BlockManager, FormatOptions};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_loopback_path(name: &str) -> std::path::PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!("n1-arc-raw-index-{}-{}.img", name, stamp))
    }

    #[tokio::test]
    async fn raw_index_and_blob_can_share_single_raw_device() {
        let _guard = crate::core::disk_manager::STORAGE_TEST_LOCK.lock().await;
        let path = unique_loopback_path("shared");
        let total_bytes = 512 * 1024 * 1024u64;
        BlockManager::format(
            &path,
            FormatOptions {
                total_bytes: Some(total_bytes),
                ..Default::default()
            },
        )
        .expect("format loopback raw device");

        let raw = path.to_string_lossy().to_string();
        let fs_root = std::env::temp_dir()
            .join("n1-arc-raw-index-sidecar-tests")
            .to_string_lossy()
            .to_string();
        crate::core::disk_manager::replace_storage_roots_for_tests(
            vec![raw.clone()],
            vec![raw],
            fs_root,
        );
        crate::core::blob_store::clear_raw_runtime_caches_for_tests();
        clear_runtime_caches_for_tests().await;

        ensure_initialized()
            .await
            .expect("initialize raw index runtime");

        let fragment = "frag_shared_raw".to_string();
        let manifest = UploadSessionManifest {
            fragment: fragment.clone(),
            object_key: "obj.bin".to_string(),
            version_id: "v1".to_string(),
            session_id: "s1".to_string(),
            total_size_bytes: 8192,
            expected_parts: 1,
            content_type: "application/octet-stream".to_string(),
            created_at: chrono::Utc::now().timestamp(),
            stripe_size_bytes: 262_144,
            data_shards: 1,
            parity_shards: 1,
            planned_parts: Vec::new(),
        };
        open_session(&manifest, None)
            .await
            .expect("open raw session");
        assert!(
            load_session(&fragment, "v1")
                .await
                .expect("load raw session")
                .is_some(),
            "raw session state should persist on shared raw device"
        );

        let (_raw_addr, _io) = crate::core::blob_store::allocate_raw_direct_io(0, 4096)
            .expect("allocate blob extent on shared raw device");
        upsert_quota(
            &fragment,
            &Quota {
                allocated_storage: 1024 * 1024,
                storage_used: 4096,
            },
        )
        .await
        .expect("persist raw quota");
        let quota = read_quota(&fragment)
            .await
            .expect("read raw quota")
            .expect("raw quota exists");
        assert_eq!(quota.storage_used, 4096);
    }
}
