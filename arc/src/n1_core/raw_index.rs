use crate::enums::app_error_enum::AppError;
use crate::n1_core::blob_store::open_or_format_raw_manager_for_path;
use crate::n1_core::disk_manager::{
    ObjectMetadata, Quota, compare_version_recency, configured_index_store_disks,
};
use crate::n1_core::fragment_metadata::FragmentMetadata;
use crate::n1_core::index::{IndexRuntimeStats, IndexSessionState, RemovedSessionReceipt, UploadRuntimeStats};
use crate::n1_core::upload::UploadSessionManifest;
use block_manager::{AllocationHandle, BlockManager};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock as TokioRwLock, mpsc};

const RAW_INDEX_MAGIC: &[u8; 8] = b"N1ARCIDX";
const RAW_INDEX_RECORD_HEADER_BYTES: usize = 20;
const RAW_INDEX_CATALOG_BYTES: u64 = 64 * 1024 * 1024;
const RAW_INDEX_STREAM_BUFFER: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RawIndexState {
    sessions: HashMap<String, IndexSessionState>,
    removed_sessions: HashMap<String, RemovedSessionReceipt>,
    live_versions: HashMap<String, ObjectMetadata>,
    trash_versions: HashMap<String, ObjectMetadata>,
    #[serde(default)]
    quotas: HashMap<String, Quota>,
    #[serde(default)]
    fragment_metadata: HashMap<String, FragmentMetadata>,
}

struct RawIndexDevice {
    path: PathBuf,
    manager: Arc<std::sync::RwLock<BlockManager>>,
    catalog: AllocationHandle,
}

struct RawIndexRuntime {
    devices: Vec<Arc<RawIndexDevice>>,
    generation: TokioRwLock<u64>,
    state: TokioRwLock<RawIndexState>,
}

static RAW_INDEX_RUNTIME: std::sync::LazyLock<Mutex<Option<Arc<RawIndexRuntime>>>> =
    std::sync::LazyLock::new(|| Mutex::new(None));

fn session_key(fragment: &str, version_id: &str) -> String {
    format!("{}|{}", fragment, version_id)
}

fn object_version_key(fragment: &str, object_key: &str, version_id: &str) -> String {
    format!("{}|{}|{}", fragment, object_key, version_id)
}

fn parse_raw_index_record(buf: &[u8]) -> Result<Option<(u64, RawIndexState)>, AppError> {
    if buf.len() < RAW_INDEX_RECORD_HEADER_BYTES {
        return Ok(None);
    }
    if &buf[..8] != RAW_INDEX_MAGIC {
        return Ok(None);
    }
    let generation = u64::from_le_bytes(
        buf[8..16]
            .try_into()
            .map_err(|_| AppError::InternalServerError("invalid raw index generation header".into()))?,
    );
    let payload_len = u32::from_le_bytes(
        buf[16..20]
            .try_into()
            .map_err(|_| AppError::InternalServerError("invalid raw index payload header".into()))?,
    ) as usize;
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

fn encode_raw_index_record(generation: u64, state: &RawIndexState) -> Result<Vec<u8>, AppError> {
    let payload = rmp_serde::to_vec(state)?;
    let max_payload = (RAW_INDEX_CATALOG_BYTES as usize).saturating_sub(RAW_INDEX_RECORD_HEADER_BYTES);
    if payload.len() > max_payload {
        return Err(AppError::InternalServerError(format!(
            "raw index catalog overflow: {} > {} bytes",
            payload.len(),
            max_payload
        )));
    }
    let mut out = Vec::with_capacity(RAW_INDEX_RECORD_HEADER_BYTES + payload.len());
    out.extend_from_slice(RAW_INDEX_MAGIC);
    out.extend_from_slice(&generation.to_le_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

fn detect_catalog_handle(manager: &BlockManager) -> Result<Option<AllocationHandle>, AppError> {
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
        if parse_raw_index_record(&bytes)?.is_some() {
            return Ok(Some(handle));
        }
    }
    Ok(None)
}

fn ensure_catalog_handle(manager: &mut BlockManager) -> Result<AllocationHandle, AppError> {
    if let Some(handle) = detect_catalog_handle(manager)? {
        return Ok(handle);
    }
    let handle = manager.allocate_bytes(RAW_INDEX_CATALOG_BYTES).map_err(|e| {
        AppError::InternalServerError(format!("raw index allocate catalog failed: {}", e))
    })?;
    manager.flush_meta().map_err(|e| {
        AppError::InternalServerError(format!(
            "raw index flush meta after catalog allocation failed: {}",
            e
        ))
    })?;
    Ok(handle)
}

async fn load_best_state(devices: &[Arc<RawIndexDevice>]) -> Result<(u64, RawIndexState), AppError> {
    let mut best_generation = 0u64;
    let mut best_state = RawIndexState::default();
    for device in devices {
        let manager = device.manager.read().map_err(|_| {
            AppError::InternalServerError("raw index manager read lock poisoned".into())
        })?;
        let cap = device
            .catalog
            .capacity_bytes(manager.header().block_size) as usize;
        let bytes = manager
            .read(device.catalog, 0, cap)
            .map_err(|e| AppError::InternalServerError(format!(
                "raw index read catalog {} failed: {}",
                device.path.display(),
                e
            )))?;
        if let Some((generation, state)) = parse_raw_index_record(&bytes)? {
            if generation >= best_generation {
                best_generation = generation;
                best_state = state;
            }
        }
    }
    Ok((best_generation, best_state))
}

async fn persist_state(devices: &[Arc<RawIndexDevice>], generation: u64, state: &RawIndexState) -> Result<(), AppError> {
    let bytes = encode_raw_index_record(generation, state)?;
    for device in devices {
        let mut manager = device.manager.write().map_err(|_| {
            AppError::InternalServerError("raw index manager write lock poisoned".into())
        })?;
        manager
            .write(device.catalog, 0, &bytes)
            .map_err(|e| AppError::InternalServerError(format!(
                "raw index write catalog {} failed: {}",
                device.path.display(),
                e
            )))?;
        manager
            .flush_data()
            .map_err(|e| AppError::InternalServerError(format!(
                "raw index flush data {} failed: {}",
                device.path.display(),
                e
            )))?;
        manager
            .flush_meta()
            .map_err(|e| AppError::InternalServerError(format!(
                "raw index flush meta {} failed: {}",
                device.path.display(),
                e
            )))?;
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
        let canonical = std::fs::canonicalize(&raw_path).unwrap_or_else(|_| PathBuf::from(&raw_path));
        if !seen.insert(canonical.clone()) {
            continue;
        }

        let manager = open_or_format_raw_manager_for_path(Path::new(&raw_path))?;
        let catalog = {
            let mut mgr = manager.write().map_err(|_| {
                AppError::InternalServerError("raw index manager write lock poisoned".into())
            })?;
            ensure_catalog_handle(&mut mgr)?
        };

        devices.push(Arc::new(RawIndexDevice {
            path: canonical,
            manager,
            catalog,
        }));
    }

    let (generation, state) = load_best_state(&devices).await?;
    persist_state(&devices, generation.max(1), &state).await?;

    let rt = Arc::new(RawIndexRuntime {
        devices,
        generation: TokioRwLock::new(generation.max(1)),
        state: TokioRwLock::new(state),
    });
    *guard = Some(Arc::clone(&rt));
    Ok(rt)
}

pub async fn ensure_initialized() -> Result<(), AppError> {
    let _ = runtime().await?;
    Ok(())
}

async fn persist_with_mutation<F, T>(mutate: F) -> Result<T, AppError>
where
    F: FnOnce(&mut RawIndexState) -> Result<T, AppError>,
{
    let rt = runtime().await?;
    let mut state_guard = rt.state.write().await;
    let out = mutate(&mut state_guard)?;
    let mut gen_guard = rt.generation.write().await;
    *gen_guard = gen_guard.saturating_add(1);
    let generation = *gen_guard;
    let snapshot = state_guard.clone();
    drop(gen_guard);
    drop(state_guard);
    persist_state(&rt.devices, generation, &snapshot).await?;
    Ok(out)
}

pub async fn open_session(
    manifest: &UploadSessionManifest,
    reservation: Option<crate::n1_core::disk_manager::ReservationReceipt>,
) -> Result<(), AppError> {
    let session = IndexSessionState {
        manifest: manifest.clone(),
        reservation,
        parts: BTreeMap::new(),
        last_activity_at: chrono::Utc::now().timestamp(),
        finalized_at: None,
        aborted_at: None,
    };
    persist_with_mutation(|state| {
        state
            .sessions
            .insert(session_key(&manifest.fragment, &manifest.version_id), session);
        Ok(())
    })
    .await
}

pub async fn commit_session_part(
    fragment: &str,
    version_id: &str,
    part: &crate::n1_core::disk_manager::PartMetadata,
) -> Result<(), AppError> {
    persist_with_mutation(|state| {
        let key = session_key(fragment, version_id);
        let session = state.sessions.get_mut(&key).ok_or(AppError::NotFound)?;
        session.parts.insert(part.part_index, part.clone());
        session.last_activity_at = chrono::Utc::now().timestamp();
        Ok(())
    })
    .await
}

pub async fn load_session(fragment: &str, version_id: &str) -> Result<Option<IndexSessionState>, AppError> {
    let rt = runtime().await?;
    let state = rt.state.read().await;
    Ok(state
        .sessions
        .get(&session_key(fragment, version_id))
        .cloned())
}

pub async fn remove_session(fragment: &str, version_id: &str, reason: &str) -> Result<(), AppError> {
    persist_with_mutation(|state| {
        let key = session_key(fragment, version_id);
        let removed = state.sessions.remove(&key).ok_or(AppError::NotFound)?;
        let receipt = RemovedSessionReceipt {
            state: removed,
            removed_at: chrono::Utc::now().timestamp(),
            reason: reason.to_string(),
        };
        state.removed_sessions.insert(key, receipt);
        Ok(())
    })
    .await
}

pub async fn list_active_sessions(fragment: &str) -> Result<Vec<IndexSessionState>, AppError> {
    let rt = runtime().await?;
    let state = rt.state.read().await;
    Ok(state
        .sessions
        .values()
        .filter(|s| s.manifest.fragment == fragment)
        .cloned()
        .collect())
}

pub async fn read_removed_session_receipt(
    fragment: &str,
    version_id: &str,
) -> Result<Option<RemovedSessionReceipt>, AppError> {
    let rt = runtime().await?;
    let state = rt.state.read().await;
    Ok(state
        .removed_sessions
        .get(&session_key(fragment, version_id))
        .cloned())
}

pub async fn delete_removed_session_receipt(fragment: &str, version_id: &str) -> Result<(), AppError> {
    persist_with_mutation(|state| {
        state
            .removed_sessions
            .remove(&session_key(fragment, version_id));
        Ok(())
    })
    .await
}

fn choose_latest_for_key<'a>(versions: impl Iterator<Item = &'a ObjectMetadata>) -> Option<ObjectMetadata> {
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

pub async fn upsert_object_version(metadata: &ObjectMetadata, trashed: bool) -> Result<(), AppError> {
    persist_with_mutation(|state| {
        let key = object_version_key(&metadata.fragment, &metadata.object_key, &metadata.version_id);
        if trashed {
            state.trash_versions.insert(key, metadata.clone());
        } else {
            state.live_versions.insert(key, metadata.clone());
        }
        Ok(())
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

pub async fn read_latest_object(fragment: &str, object_key: &str) -> Result<ObjectMetadata, AppError> {
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
    Ok(list_latest_objects_internal(fragment, trashed, None).await?.len() as u64)
}

pub async fn list_fragment_latest_objects_page(
    fragment: &str,
    trashed: bool,
    prefix: Option<&str>,
    start_after: Option<(&str, &str)>,
    limit: usize,
) -> Result<(Vec<ObjectMetadata>, Option<(String, String)>), AppError> {
    let mut all = list_latest_objects_internal(fragment, trashed, prefix).await?;
    if let Some((start_key, start_version)) = start_after {
        all.retain(|m| {
            (m.object_key.as_str(), m.version_id.as_str()) > (start_key, start_version)
        });
    }
    let capped_limit = limit.max(1);
    let page: Vec<ObjectMetadata> = all.into_iter().take(capped_limit).collect();
    let next = page
        .last()
        .map(|m| (m.object_key.clone(), m.version_id.clone()));
    Ok((page, next))
}

pub async fn list_fragment_objects(fragment: &str, prefix: Option<&str>, trashed: bool) -> Result<Vec<ObjectMetadata>, AppError> {
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
    persist_with_mutation(|state| {
        let key = object_version_key(fragment, object_key, version_id);
        let removed = if trashed {
            state.trash_versions.remove(&key).is_some()
        } else {
            state.live_versions.remove(&key).is_some()
        };
        Ok(removed)
    })
    .await
}

pub async fn move_object_version_to_trash(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    persist_with_mutation(|state| {
        let key = object_version_key(fragment, object_key, version_id);
        let Some(meta) = state.live_versions.remove(&key) else {
            return Ok(None);
        };
        state.trash_versions.insert(key, meta.clone());
        Ok(Some(meta))
    })
    .await
}

pub async fn restore_object_version_from_trash(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    persist_with_mutation(|state| {
        let key = object_version_key(fragment, object_key, version_id);
        let Some(meta) = state.trash_versions.remove(&key) else {
            return Ok(None);
        };
        state.live_versions.insert(key, meta.clone());
        Ok(Some(meta))
    })
    .await
}

pub async fn clear_object_latest_states(fragment: &str, object_key: &str) -> Result<(), AppError> {
    persist_with_mutation(|state| {
        state
            .live_versions
            .retain(|_, m| !(m.fragment == fragment && m.object_key == object_key));
        state
            .trash_versions
            .retain(|_, m| !(m.fragment == fragment && m.object_key == object_key));
        Ok(())
    })
    .await
}

pub async fn clear_fragment_trash_in_partitions(
    fragment: &str,
    _touched_partitions: &BTreeSet<u16>,
) -> Result<usize, AppError> {
    persist_with_mutation(|state| {
        let before = state.trash_versions.len();
        state.trash_versions.retain(|_, m| m.fragment != fragment);
        Ok(before.saturating_sub(state.trash_versions.len()))
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
            for sess in state.sessions.values() {
                active_fragments.insert(sess.manifest.fragment.clone());
                committed_parts = committed_parts.saturating_add(sess.parts.len() as u64);
                committed_bytes = committed_bytes.saturating_add(
                    sess.parts.values().map(|p| p.size_bytes).sum::<u64>(),
                );
            }
            UploadRuntimeStats {
                active_fragments: active_fragments.len() as u64,
                active_sessions: state.sessions.len() as u64,
                reserved_bytes: 0,
                committed_parts,
                committed_bytes,
                oldest_session_age_seconds: None,
            }
        }
        Err(_) => UploadRuntimeStats::default(),
    }
}

pub async fn collect_index_runtime_stats() -> IndexRuntimeStats {
    match runtime().await {
        Ok(rt) => IndexRuntimeStats {
            loaded_partition_runtimes: 1,
            wal_appenders: 0,
            prefix_projection_fragments_pending: rt.devices.len() as u64,
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
    for meta in state.live_versions.values() {
        fragments.insert(meta.fragment.clone());
    }
    for meta in state.trash_versions.values() {
        fragments.insert(meta.fragment.clone());
    }
    Ok(fragments.into_iter().collect())
}

pub async fn read_quota(fragment: &str) -> Result<Option<Quota>, AppError> {
    let rt = runtime().await?;
    let state = rt.state.read().await;
    Ok(state.quotas.get(fragment).cloned())
}

pub async fn upsert_quota(fragment: &str, quota: &Quota) -> Result<(), AppError> {
    persist_with_mutation(|state| {
        state.quotas.insert(fragment.to_string(), quota.clone());
        Ok(())
    })
    .await
}

pub async fn read_fragment_metadata(fragment: &str) -> Result<Option<FragmentMetadata>, AppError> {
    let rt = runtime().await?;
    let state = rt.state.read().await;
    Ok(state.fragment_metadata.get(fragment).cloned())
}

pub async fn upsert_fragment_metadata(meta: &FragmentMetadata) -> Result<(), AppError> {
    persist_with_mutation(|state| {
        state
            .fragment_metadata
            .insert(meta.fragment.clone(), meta.clone());
        Ok(())
    })
    .await
}

pub async fn delete_fragment_state(fragment: &str) -> Result<(), AppError> {
    persist_with_mutation(|state| {
        state.fragment_metadata.remove(fragment);
        state.quotas.remove(fragment);
        state.sessions.retain(|_, s| s.manifest.fragment != fragment);
        state.removed_sessions.retain(|_, r| r.state.manifest.fragment != fragment);
        state.live_versions.retain(|_, m| m.fragment != fragment);
        state.trash_versions.retain(|_, m| m.fragment != fragment);
        Ok(())
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
        crate::n1_core::disk_manager::replace_storage_roots_for_tests(
            vec![raw.clone()],
            vec![raw],
            fs_root,
        );
        crate::n1_core::blob_store::clear_raw_runtime_caches_for_tests();
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

        let (_raw_addr, _io) = crate::n1_core::blob_store::allocate_raw_direct_io(0, 4096)
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
