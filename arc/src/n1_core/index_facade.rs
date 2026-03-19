use crate::enums::app_error_enum::AppError;
use crate::n1_core::disk_manager::{ObjectMetadata, PartMetadata, ReservationReceipt};
pub use crate::n1_core::index_types::{
    IndexReplicaWriteMode, IndexRuntimeStats, IndexSessionState, RemovedSessionReceipt,
    SessionPartReceipt, UploadRuntimeStats,
};
use crate::n1_core::raw_index;
use crate::n1_core::upload::UploadSessionManifest;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::path::PathBuf;
use tokio::sync::mpsc;

async fn ensure_raw_backend(_op: &'static str) -> Result<(), AppError> {
    raw_index::ensure_initialized().await
}

pub async fn open_session(
    manifest: &UploadSessionManifest,
    reservation: Option<ReservationReceipt>,
) -> Result<(), AppError> {
    ensure_raw_backend("open_session").await?;
    raw_index::open_session(manifest, reservation).await
}

#[allow(dead_code)]
pub async fn commit_session_part(
    fragment: &str,
    version_id: &str,
    part: &PartMetadata,
    replica_write_mode: IndexReplicaWriteMode,
) -> Result<(), AppError> {
    let _ = replica_write_mode;
    ensure_raw_backend("commit_session_part").await?;
    raw_index::commit_session_part(fragment, version_id, part).await
}

pub async fn commit_session_part_receipt(
    fragment: &str,
    version_id: &str,
    receipt: &SessionPartReceipt,
) -> Result<(), AppError> {
    ensure_raw_backend("commit_session_part_receipt").await?;
    raw_index::commit_session_part_receipt(fragment, version_id, receipt).await
}

pub async fn load_session(
    fragment: &str,
    version_id: &str,
) -> Result<Option<IndexSessionState>, AppError> {
    ensure_raw_backend("load_session").await?;
    raw_index::load_session(fragment, version_id).await
}

pub async fn load_session_manifest(
    fragment: &str,
    version_id: &str,
) -> Result<Option<UploadSessionManifest>, AppError> {
    ensure_raw_backend("load_session_manifest").await?;
    raw_index::load_session_manifest(fragment, version_id).await
}

pub async fn read_session_part(
    fragment: &str,
    version_id: &str,
    part_index: u64,
) -> Result<Option<PartMetadata>, AppError> {
    ensure_raw_backend("read_session_part").await?;
    raw_index::read_session_part(fragment, version_id, part_index).await
}

pub async fn read_session_part_receipt(
    fragment: &str,
    version_id: &str,
    part_index: u64,
) -> Result<Option<SessionPartReceipt>, AppError> {
    ensure_raw_backend("read_session_part_receipt").await?;
    raw_index::read_session_part_receipt(fragment, version_id, part_index).await
}

pub async fn load_session_part_receipts(
    fragment: &str,
    version_id: &str,
    manifest: &UploadSessionManifest,
) -> Result<Option<Vec<Option<SessionPartReceipt>>>, AppError> {
    ensure_raw_backend("load_session_part_receipts").await?;
    raw_index::load_session_part_receipts(fragment, version_id, manifest).await
}

pub async fn remove_session(
    fragment: &str,
    version_id: &str,
    reason: &str,
) -> Result<(), AppError> {
    ensure_raw_backend("remove_session").await?;
    raw_index::remove_session(fragment, version_id, reason).await
}

pub async fn list_active_sessions(fragment: &str) -> Result<Vec<IndexSessionState>, AppError> {
    ensure_raw_backend("list_active_sessions").await?;
    raw_index::list_active_sessions(fragment).await
}

pub async fn read_removed_session_receipt(
    fragment: &str,
    version_id: &str,
) -> Result<Option<RemovedSessionReceipt>, AppError> {
    ensure_raw_backend("read_removed_session_receipt").await?;
    raw_index::read_removed_session_receipt(fragment, version_id).await
}

pub async fn list_removed_session_receipts(
    fragment: &str,
) -> Result<Vec<(String, RemovedSessionReceipt)>, AppError> {
    ensure_raw_backend("list_removed_session_receipts").await?;
    raw_index::list_removed_session_receipts(fragment).await
}

pub async fn delete_removed_session_receipt(
    fragment: &str,
    version_id: &str,
) -> Result<(), AppError> {
    ensure_raw_backend("delete_removed_session_receipt").await?;
    raw_index::delete_removed_session_receipt(fragment, version_id).await
}

pub async fn publish_object(metadata: &ObjectMetadata) -> Result<(), AppError> {
    ensure_raw_backend("publish_object").await?;
    raw_index::publish_object(metadata).await
}

pub async fn read_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<ObjectMetadata, AppError> {
    ensure_raw_backend("read_object_version").await?;
    raw_index::read_object_version(fragment, object_key, version_id, false).await
}

pub async fn read_trashed_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<ObjectMetadata, AppError> {
    ensure_raw_backend("read_trashed_object_version").await?;
    raw_index::read_object_version(fragment, object_key, version_id, true).await
}

pub async fn read_latest_object(
    fragment: &str,
    object_key: &str,
) -> Result<ObjectMetadata, AppError> {
    ensure_raw_backend("read_latest_object").await?;
    raw_index::read_latest_object(fragment, object_key).await
}

pub async fn count_fragment_latest_objects(fragment: &str, trashed: bool) -> Result<u64, AppError> {
    ensure_raw_backend("count_fragment_latest_objects").await?;
    raw_index::count_fragment_latest_objects(fragment, trashed).await
}

pub async fn list_fragments() -> Result<Vec<String>, AppError> {
    ensure_raw_backend("list_fragments").await?;
    raw_index::list_fragments().await
}

pub async fn collect_runtime_upload_stats() -> UploadRuntimeStats {
    match ensure_raw_backend("collect_runtime_upload_stats").await {
        Ok(()) => raw_index::collect_runtime_upload_stats().await,
        Err(_) => UploadRuntimeStats::default(),
    }
}

pub async fn collect_index_runtime_stats() -> IndexRuntimeStats {
    match ensure_raw_backend("collect_index_runtime_stats").await {
        Ok(()) => raw_index::collect_index_runtime_stats().await,
        Err(_) => IndexRuntimeStats::default(),
    }
}

pub fn stream_fragment_object_versions(
    fragment: String,
    trashed: bool,
) -> mpsc::Receiver<Result<ObjectMetadata, AppError>> {
    let (tx, rx) = mpsc::channel(64);
    tokio::spawn(async move {
        match ensure_raw_backend("stream_fragment_object_versions").await {
            Ok(()) => {
                let mut src = raw_index::stream_fragment_object_versions(fragment, trashed).await;
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
    ensure_raw_backend("list_fragment_objects").await?;
    raw_index::list_fragment_objects(fragment, None, false).await
}

pub async fn list_fragment_objects_for_prefix(
    fragment: &str,
    prefix: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    ensure_raw_backend("list_fragment_objects_for_prefix").await?;
    raw_index::list_fragment_objects(fragment, Some(prefix), false).await
}

pub async fn list_fragment_trash_objects(fragment: &str) -> Result<Vec<ObjectMetadata>, AppError> {
    ensure_raw_backend("list_fragment_trash_objects").await?;
    raw_index::list_fragment_objects(fragment, None, true).await
}

pub async fn list_fragment_trash_objects_for_prefix(
    fragment: &str,
    prefix: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    ensure_raw_backend("list_fragment_trash_objects_for_prefix").await?;
    raw_index::list_fragment_objects(fragment, Some(prefix), true).await
}

pub async fn list_fragment_latest_objects_page(
    fragment: &str,
    trashed: bool,
    prefix: Option<&str>,
    start_after: Option<(&str, &str)>,
    limit: usize,
) -> Result<(Vec<ObjectMetadata>, Option<(String, String)>), AppError> {
    ensure_raw_backend("list_fragment_latest_objects_page").await?;
    raw_index::list_fragment_latest_objects_page(fragment, trashed, prefix, start_after, limit)
        .await
}

pub async fn list_live_object_versions_metadata(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    ensure_raw_backend("list_live_object_versions_metadata").await?;
    raw_index::list_object_versions(fragment, object_key, false).await
}

pub async fn list_trashed_object_versions_metadata(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    ensure_raw_backend("list_trashed_object_versions_metadata").await?;
    raw_index::list_object_versions(fragment, object_key, true).await
}

pub async fn upsert_object_version(metadata: &ObjectMetadata) -> Result<(), AppError> {
    ensure_raw_backend("upsert_object_version").await?;
    raw_index::upsert_object_version(metadata, false).await
}

pub async fn upsert_object_version_with_replica_mode(
    metadata: &ObjectMetadata,
    replica_write_mode: IndexReplicaWriteMode,
) -> Result<(), AppError> {
    let _ = replica_write_mode;
    ensure_raw_backend("upsert_object_version_with_replica_mode").await?;
    raw_index::upsert_object_version(metadata, false).await
}

pub async fn upsert_object_version_on_available_roots_with_replica_mode(
    metadata: &ObjectMetadata,
    replica_write_mode: IndexReplicaWriteMode,
) -> Result<(), AppError> {
    let _ = replica_write_mode;
    ensure_raw_backend("upsert_object_version_on_available_roots_with_replica_mode").await?;
    raw_index::upsert_object_version(metadata, false).await
}

pub async fn upsert_trashed_object_version(metadata: &ObjectMetadata) -> Result<(), AppError> {
    ensure_raw_backend("upsert_trashed_object_version").await?;
    raw_index::upsert_object_version(metadata, true).await
}

pub async fn upsert_trashed_object_version_with_replica_mode(
    metadata: &ObjectMetadata,
    replica_write_mode: IndexReplicaWriteMode,
) -> Result<(), AppError> {
    let _ = replica_write_mode;
    ensure_raw_backend("upsert_trashed_object_version_with_replica_mode").await?;
    raw_index::upsert_object_version(metadata, true).await
}

pub async fn upsert_trashed_object_version_on_available_roots_with_replica_mode(
    metadata: &ObjectMetadata,
    replica_write_mode: IndexReplicaWriteMode,
) -> Result<(), AppError> {
    let _ = replica_write_mode;
    ensure_raw_backend("upsert_trashed_object_version_on_available_roots_with_replica_mode")
        .await?;
    raw_index::upsert_object_version(metadata, true).await
}

pub async fn delete_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<bool, AppError> {
    ensure_raw_backend("delete_object_version").await?;
    raw_index::delete_object_version(fragment, object_key, version_id, false).await
}

pub async fn delete_trashed_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<bool, AppError> {
    ensure_raw_backend("delete_trashed_object_version").await?;
    raw_index::delete_object_version(fragment, object_key, version_id, true).await
}

pub async fn move_object_version_to_trash(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    ensure_raw_backend("move_object_version_to_trash").await?;
    raw_index::move_object_version_to_trash(fragment, object_key, version_id).await
}

pub async fn restore_object_version_from_trash(
    fragment: &str,
    object_key: &str,
    version_id: &str,
    allow_on_available_roots: bool,
) -> Result<Option<ObjectMetadata>, AppError> {
    let _ = allow_on_available_roots;
    ensure_raw_backend("restore_object_version_from_trash").await?;
    raw_index::restore_object_version_from_trash(fragment, object_key, version_id).await
}

pub async fn clear_object_latest_states(fragment: &str, object_key: &str) -> Result<(), AppError> {
    ensure_raw_backend("clear_object_latest_states").await?;
    raw_index::clear_object_latest_states(fragment, object_key).await
}

pub async fn clear_fragment_trash_in_partitions(
    fragment: &str,
    touched_partitions: &BTreeSet<u16>,
) -> Result<usize, AppError> {
    ensure_raw_backend("clear_fragment_trash_in_partitions").await?;
    raw_index::clear_fragment_trash_in_partitions(fragment, touched_partitions).await
}

pub async fn is_prefix_projection_pending(fragment: &str) -> bool {
    match ensure_raw_backend("is_prefix_projection_pending").await {
        Ok(()) => raw_index::is_prefix_projection_pending(fragment).await,
        Err(_) => false,
    }
}

#[cfg(test)]
pub fn get_session_state_paths(fragment: &str, version_id: &str) -> Result<Vec<PathBuf>, AppError> {
    let _ = (fragment, version_id);
    Err(AppError::BadRequest(
        "filesystem session paths are not available in raw-only Arc".into(),
    ))
}

pub fn get_object_version_paths(
    fragment: &str,
    object_key: &str,
    version_id: &str,
    trashed: bool,
) -> Result<Vec<PathBuf>, AppError> {
    let _ = (fragment, object_key, version_id, trashed);
    Err(AppError::BadRequest(
        "filesystem metadata paths are not available in raw-only Arc".into(),
    ))
}

pub(crate) fn object_partition_id(fragment: &str, object_key: &str) -> Result<u16, AppError> {
    let mut hasher = Sha256::new();
    for part in ["object", fragment, object_key] {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    let hash: [u8; 32] = hasher.finalize().into();
    Ok((u16::from(hash[0]) << 8 | u16::from(hash[1])) % 256)
}

#[cfg(test)]
pub async fn clear_runtime_caches_for_tests() {
    raw_index::clear_runtime_caches_for_tests().await;
}
