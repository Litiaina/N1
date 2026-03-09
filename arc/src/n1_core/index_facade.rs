use crate::enums::app_error_enum::AppError;
use crate::n1_core::disk_manager::{
    ObjectMetadata, ObjectPathManager, PartMetadata, ReservationReceipt,
};
use crate::n1_core::index;
use crate::n1_core::raw_index;
use crate::n1_core::upload::UploadSessionManifest;
use once_cell::sync::Lazy;
use std::collections::{BTreeSet, HashSet};
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, Ordering};
use tokio::sync::mpsc;
use tracing::warn;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexBackendMode {
    Fs = 0,
    RawShadow = 1,
    RawOnly = 2,
}

static INDEX_BACKEND_MODE: AtomicU8 = AtomicU8::new(IndexBackendMode::Fs as u8);
static RAW_SHADOW_WARNED: Lazy<Mutex<HashSet<&'static str>>> = Lazy::new(|| Mutex::new(HashSet::new()));

pub use index::{
    IndexReplicaWriteMode, IndexRuntimeStats, IndexSessionState, RemovedSessionReceipt,
    UploadRuntimeStats,
};

pub fn set_index_backend_mode(mode: IndexBackendMode) {
    INDEX_BACKEND_MODE.store(mode as u8, Ordering::Relaxed);
}

pub fn index_backend_mode() -> IndexBackendMode {
    match INDEX_BACKEND_MODE.load(Ordering::Relaxed) {
        1 => IndexBackendMode::RawShadow,
        2 => IndexBackendMode::RawOnly,
        _ => IndexBackendMode::Fs,
    }
}

async fn use_raw_backend(op: &'static str) -> Result<bool, AppError> {
    match index_backend_mode() {
        IndexBackendMode::Fs => Ok(false),
        IndexBackendMode::RawOnly => {
            raw_index::ensure_initialized().await?;
            Ok(true)
        }
        IndexBackendMode::RawShadow => {
            raw_index::ensure_initialized().await?;
            let mut warned = RAW_SHADOW_WARNED
                .lock()
                .map_err(|_| AppError::InternalServerError("raw-shadow warning lock poisoned".into()))?;
            if warned.insert(op) {
                warn!(
                    "Arc raw-shadow mode op={} currently uses raw-index as authority; fs shadow write is not enabled yet",
                    op
                );
            }
            Ok(true)
        }
    }
}

pub async fn open_session(
    manifest: &UploadSessionManifest,
    reservation: Option<ReservationReceipt>,
) -> Result<(), AppError> {
    if use_raw_backend("open_session").await? {
        raw_index::open_session(manifest, reservation).await
    } else {
        index::open_session(manifest, reservation).await
    }
}

pub async fn commit_session_part(
    fragment: &str,
    version_id: &str,
    part: &PartMetadata,
    replica_write_mode: IndexReplicaWriteMode,
) -> Result<(), AppError> {
    if use_raw_backend("commit_session_part").await? {
        let _ = replica_write_mode;
        raw_index::commit_session_part(fragment, version_id, part).await
    } else {
        index::commit_session_part(fragment, version_id, part, replica_write_mode).await
    }
}

pub async fn load_session(fragment: &str, version_id: &str) -> Result<Option<IndexSessionState>, AppError> {
    if use_raw_backend("load_session").await? {
        raw_index::load_session(fragment, version_id).await
    } else {
        index::load_session(fragment, version_id).await
    }
}

pub async fn remove_session(
    fragment: &str,
    version_id: &str,
    reason: &str,
) -> Result<(), AppError> {
    if use_raw_backend("remove_session").await? {
        raw_index::remove_session(fragment, version_id, reason).await
    } else {
        index::remove_session(fragment, version_id, reason).await
    }
}

pub async fn list_active_sessions(fragment: &str) -> Result<Vec<IndexSessionState>, AppError> {
    if use_raw_backend("list_active_sessions").await? {
        raw_index::list_active_sessions(fragment).await
    } else {
        index::list_active_sessions(fragment).await
    }
}

pub async fn read_removed_session_receipt(
    fragment: &str,
    version_id: &str,
) -> Result<Option<RemovedSessionReceipt>, AppError> {
    if use_raw_backend("read_removed_session_receipt").await? {
        raw_index::read_removed_session_receipt(fragment, version_id).await
    } else {
        index::read_removed_session_receipt(fragment, version_id).await
    }
}

pub async fn delete_removed_session_receipt(
    fragment: &str,
    version_id: &str,
) -> Result<(), AppError> {
    if use_raw_backend("delete_removed_session_receipt").await? {
        raw_index::delete_removed_session_receipt(fragment, version_id).await
    } else {
        index::delete_removed_session_receipt(fragment, version_id).await
    }
}

pub async fn publish_object(metadata: &ObjectMetadata) -> Result<(), AppError> {
    if use_raw_backend("publish_object").await? {
        raw_index::publish_object(metadata).await
    } else {
        index::publish_object(metadata).await
    }
}

pub async fn read_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<ObjectMetadata, AppError> {
    if use_raw_backend("read_object_version").await? {
        raw_index::read_object_version(fragment, object_key, version_id, false).await
    } else {
        index::read_object_version(fragment, object_key, version_id).await
    }
}

pub async fn read_trashed_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<ObjectMetadata, AppError> {
    if use_raw_backend("read_trashed_object_version").await? {
        raw_index::read_object_version(fragment, object_key, version_id, true).await
    } else {
        index::read_trashed_object_version(fragment, object_key, version_id).await
    }
}

pub async fn read_latest_object(fragment: &str, object_key: &str) -> Result<ObjectMetadata, AppError> {
    if use_raw_backend("read_latest_object").await? {
        raw_index::read_latest_object(fragment, object_key).await
    } else {
        index::read_latest_object(fragment, object_key).await
    }
}

pub async fn count_fragment_latest_objects(fragment: &str, trashed: bool) -> Result<u64, AppError> {
    if use_raw_backend("count_fragment_latest_objects").await? {
        raw_index::count_fragment_latest_objects(fragment, trashed).await
    } else {
        index::count_fragment_latest_objects(fragment, trashed).await
    }
}

fn is_fragment_hash(name: &str) -> bool {
    if name.len() != 64 {
        return false;
    }
    name.as_bytes()
        .iter()
        .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F'))
}

pub async fn list_fragments() -> Result<Vec<String>, AppError> {
    if use_raw_backend("list_fragments").await? {
        return raw_index::list_fragments().await;
    }

    let roots = ObjectPathManager::new().get_all_index_roots();
    tokio::task::spawn_blocking(move || -> Result<Vec<String>, std::io::Error> {
        let mut fragments = BTreeSet::new();
        for root in roots {
            if !root.exists() {
                continue;
            }
            if let Ok(rd) = std::fs::read_dir(&root) {
                for ent in rd.filter_map(|e| e.ok()) {
                    if !ent.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                        continue;
                    }
                    if let Some(name) = ent.file_name().to_str() {
                        if is_fragment_hash(name) {
                            fragments.insert(name.to_string());
                        }
                    }
                }
            }
        }
        Ok(fragments.into_iter().collect())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("Task join error: {}", e)))?
    .map_err(|e| AppError::Io(e, "list fragments".into()))
}

pub async fn collect_runtime_upload_stats() -> index::UploadRuntimeStats {
    match use_raw_backend("collect_runtime_upload_stats").await {
        Ok(true) => raw_index::collect_runtime_upload_stats().await,
        Ok(false) => index::collect_runtime_upload_stats().await,
        Err(_) => UploadRuntimeStats::default(),
    }
}

pub async fn collect_index_runtime_stats() -> index::IndexRuntimeStats {
    match use_raw_backend("collect_index_runtime_stats").await {
        Ok(true) => raw_index::collect_index_runtime_stats().await,
        Ok(false) => index::collect_index_runtime_stats().await,
        Err(_) => IndexRuntimeStats::default(),
    }
}

pub fn stream_fragment_object_versions(
    fragment: String,
    trashed: bool,
) -> mpsc::Receiver<Result<ObjectMetadata, AppError>> {
    let (tx, rx) = mpsc::channel(64);
    tokio::spawn(async move {
        match use_raw_backend("stream_fragment_object_versions").await {
            Ok(true) => {
                let mut src = raw_index::stream_fragment_object_versions(fragment, trashed).await;
                while let Some(msg) = src.recv().await {
                    if tx.send(msg).await.is_err() {
                        break;
                    }
                }
            }
            Ok(false) => {
                let mut src = index::stream_fragment_object_versions(fragment, trashed);
                while let Some(msg) = src.recv().await {
                    if tx.send(msg).await.is_err() {
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

pub async fn list_fragment_objects(fragment: &str) -> Result<Vec<ObjectMetadata>, AppError> {
    if use_raw_backend("list_fragment_objects").await? {
        raw_index::list_fragment_objects(fragment, None, false).await
    } else {
        index::list_fragment_objects(fragment).await
    }
}

pub async fn list_fragment_objects_for_prefix(
    fragment: &str,
    prefix: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    if use_raw_backend("list_fragment_objects_for_prefix").await? {
        raw_index::list_fragment_objects(fragment, Some(prefix), false).await
    } else {
        index::list_fragment_objects_for_prefix(fragment, prefix).await
    }
}

pub async fn list_fragment_trash_objects(fragment: &str) -> Result<Vec<ObjectMetadata>, AppError> {
    if use_raw_backend("list_fragment_trash_objects").await? {
        raw_index::list_fragment_objects(fragment, None, true).await
    } else {
        index::list_fragment_trash_objects(fragment).await
    }
}

pub async fn list_fragment_trash_objects_for_prefix(
    fragment: &str,
    prefix: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    if use_raw_backend("list_fragment_trash_objects_for_prefix").await? {
        raw_index::list_fragment_objects(fragment, Some(prefix), true).await
    } else {
        index::list_fragment_trash_objects_for_prefix(fragment, prefix).await
    }
}

pub async fn list_fragment_latest_objects_page(
    fragment: &str,
    trashed: bool,
    prefix: Option<&str>,
    start_after: Option<(&str, &str)>,
    limit: usize,
) -> Result<(Vec<ObjectMetadata>, Option<(String, String)>), AppError> {
    if use_raw_backend("list_fragment_latest_objects_page").await? {
        raw_index::list_fragment_latest_objects_page(fragment, trashed, prefix, start_after, limit).await
    } else {
        index::list_fragment_latest_objects_page(fragment, trashed, prefix, start_after, limit).await
    }
}

pub async fn list_live_object_versions_metadata(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    if use_raw_backend("list_live_object_versions_metadata").await? {
        raw_index::list_object_versions(fragment, object_key, false).await
    } else {
        index::list_live_object_versions_metadata(fragment, object_key).await
    }
}

pub async fn list_trashed_object_versions_metadata(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    if use_raw_backend("list_trashed_object_versions_metadata").await? {
        raw_index::list_object_versions(fragment, object_key, true).await
    } else {
        index::list_trashed_object_versions_metadata(fragment, object_key).await
    }
}

pub async fn upsert_object_version(metadata: &ObjectMetadata) -> Result<(), AppError> {
    if use_raw_backend("upsert_object_version").await? {
        raw_index::upsert_object_version(metadata, false).await
    } else {
        index::upsert_object_version(metadata).await
    }
}

pub async fn upsert_object_version_with_replica_mode(
    metadata: &ObjectMetadata,
    replica_write_mode: IndexReplicaWriteMode,
) -> Result<(), AppError> {
    if use_raw_backend("upsert_object_version_with_replica_mode").await? {
        let _ = replica_write_mode;
        raw_index::upsert_object_version(metadata, false).await
    } else {
        index::upsert_object_version_with_replica_mode(metadata, replica_write_mode).await
    }
}

pub async fn upsert_object_version_on_available_roots_with_replica_mode(
    metadata: &ObjectMetadata,
    replica_write_mode: IndexReplicaWriteMode,
) -> Result<(), AppError> {
    if use_raw_backend("upsert_object_version_on_available_roots_with_replica_mode").await? {
        let _ = replica_write_mode;
        raw_index::upsert_object_version(metadata, false).await
    } else {
        index::upsert_object_version_on_available_roots_with_replica_mode(metadata, replica_write_mode)
            .await
    }
}

pub async fn upsert_trashed_object_version(metadata: &ObjectMetadata) -> Result<(), AppError> {
    if use_raw_backend("upsert_trashed_object_version").await? {
        raw_index::upsert_object_version(metadata, true).await
    } else {
        index::upsert_trashed_object_version(metadata).await
    }
}

pub async fn upsert_trashed_object_version_with_replica_mode(
    metadata: &ObjectMetadata,
    replica_write_mode: IndexReplicaWriteMode,
) -> Result<(), AppError> {
    if use_raw_backend("upsert_trashed_object_version_with_replica_mode").await? {
        let _ = replica_write_mode;
        raw_index::upsert_object_version(metadata, true).await
    } else {
        index::upsert_trashed_object_version_with_replica_mode(metadata, replica_write_mode).await
    }
}

pub async fn upsert_trashed_object_version_on_available_roots_with_replica_mode(
    metadata: &ObjectMetadata,
    replica_write_mode: IndexReplicaWriteMode,
) -> Result<(), AppError> {
    if use_raw_backend("upsert_trashed_object_version_on_available_roots_with_replica_mode").await? {
        let _ = replica_write_mode;
        raw_index::upsert_object_version(metadata, true).await
    } else {
        index::upsert_trashed_object_version_on_available_roots_with_replica_mode(
            metadata,
            replica_write_mode,
        )
        .await
    }
}

pub async fn delete_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<bool, AppError> {
    if use_raw_backend("delete_object_version").await? {
        raw_index::delete_object_version(fragment, object_key, version_id, false).await
    } else {
        index::delete_object_version(fragment, object_key, version_id).await
    }
}

pub async fn delete_trashed_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<bool, AppError> {
    if use_raw_backend("delete_trashed_object_version").await? {
        raw_index::delete_object_version(fragment, object_key, version_id, true).await
    } else {
        index::delete_trashed_object_version(fragment, object_key, version_id).await
    }
}

pub async fn move_object_version_to_trash(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    if use_raw_backend("move_object_version_to_trash").await? {
        raw_index::move_object_version_to_trash(fragment, object_key, version_id).await
    } else {
        index::move_object_version_to_trash(fragment, object_key, version_id).await
    }
}

pub async fn restore_object_version_from_trash(
    fragment: &str,
    object_key: &str,
    version_id: &str,
    allow_on_available_roots: bool,
) -> Result<Option<ObjectMetadata>, AppError> {
    if use_raw_backend("restore_object_version_from_trash").await? {
        let _ = allow_on_available_roots;
        raw_index::restore_object_version_from_trash(fragment, object_key, version_id).await
    } else {
        index::restore_object_version_from_trash(
            fragment,
            object_key,
            version_id,
            allow_on_available_roots,
        )
        .await
    }
}

pub async fn clear_object_latest_states(fragment: &str, object_key: &str) -> Result<(), AppError> {
    if use_raw_backend("clear_object_latest_states").await? {
        raw_index::clear_object_latest_states(fragment, object_key).await
    } else {
        index::clear_object_latest_states(fragment, object_key).await
    }
}

pub async fn clear_fragment_trash_in_partitions(
    fragment: &str,
    touched_partitions: &BTreeSet<u16>,
) -> Result<usize, AppError> {
    if use_raw_backend("clear_fragment_trash_in_partitions").await? {
        raw_index::clear_fragment_trash_in_partitions(fragment, touched_partitions).await
    } else {
        index::clear_fragment_trash_in_partitions(fragment, touched_partitions).await
    }
}

pub async fn is_prefix_projection_pending(fragment: &str) -> bool {
    match use_raw_backend("is_prefix_projection_pending").await {
        Ok(true) => raw_index::is_prefix_projection_pending(fragment).await,
        Ok(false) => index::is_prefix_projection_pending(fragment).await,
        Err(_) => false,
    }
}

pub fn get_session_state_paths(fragment: &str, version_id: &str) -> Result<Vec<PathBuf>, AppError> {
    match index_backend_mode() {
        IndexBackendMode::Fs => index::get_session_state_paths(fragment, version_id),
        _ => Err(AppError::BadRequest(
            "filesystem session paths are not available in raw index mode".into(),
        )),
    }
}

pub fn get_object_version_paths(
    fragment: &str,
    object_key: &str,
    version_id: &str,
    trashed: bool,
) -> Result<Vec<PathBuf>, AppError> {
    match index_backend_mode() {
        IndexBackendMode::Fs => index::get_object_version_paths(fragment, object_key, version_id, trashed),
        _ => Err(AppError::BadRequest(
            "filesystem metadata paths are not available in raw index mode".into(),
        )),
    }
}

pub(crate) fn object_partition_id(fragment: &str, object_key: &str) -> Result<u16, AppError> {
    index::object_partition_id(fragment, object_key)
}
