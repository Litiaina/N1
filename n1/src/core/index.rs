use crate::core::disk_manager::{
    ListObjectsResult, ObjectMetadata, ObjectPathManager, PartMetadata, ReservationReceipt,
};
use crate::core::indexdb_store;
use crate::core::upload::UploadSessionManifest;
use crate::enums::app_error_enum::AppError;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, LazyLock, Mutex};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

static OBJECT_OP_LOCKS: LazyLock<Mutex<HashMap<String, Arc<AsyncMutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

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
    pub part_receipts: BTreeMap<u64, SessionPartReceipt>,
    #[serde(default)]
    pub last_activity_at: i64,
    #[serde(default)]
    pub finalized_at: Option<i64>,
    #[serde(default)]
    pub aborted_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionPartReceipt {
    pub part_index: u64,
    pub size_bytes: u64,
    pub shard_size_bytes: u64,
    pub created_at: i64,
    pub checksum: Option<u32>,
    pub shard_checksums: Vec<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TrashCatalogEntry {
    pub object_key: String,
    pub version_ids: Vec<String>,
    pub deleted_at: i64,
}

pub struct ObjectOperationGuard {
    _guard: OwnedMutexGuard<()>,
}

pub async fn acquire_object_operation_lock(
    fragment: &str,
    object_key: &str,
) -> Result<ObjectOperationGuard, AppError> {
    let key = format!(
        "{}\u{001f}{}",
        fragment,
        ObjectPathManager::canonical_object_key(object_key)
    );
    let lock = {
        let mut locks = OBJECT_OP_LOCKS.lock().map_err(|_| {
            AppError::InternalServerError("object operation lock poisoned".to_string())
        })?;
        locks
            .entry(key)
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    };
    Ok(ObjectOperationGuard {
        _guard: lock.lock_owned().await,
    })
}

fn parse_crc32_hex(value: &str) -> Result<u32, AppError> {
    u32::from_str_radix(value.trim(), 16).map_err(|e| {
        AppError::InternalServerError(format!("invalid crc32 value '{}': {}", value, e))
    })
}

pub async fn open_session(
    manifest: &UploadSessionManifest,
    reservation: Option<ReservationReceipt>,
) -> Result<(), AppError> {
    indexdb_store::open_session(manifest, reservation)
}

pub async fn commit_session_part(
    fragment: &str,
    version_id: &str,
    part: &PartMetadata,
    _replica_write_mode: IndexReplicaWriteMode,
) -> Result<(), AppError> {
    indexdb_store::commit_session_part(fragment, version_id, part)
}

pub async fn commit_session_part_receipt(
    fragment: &str,
    version_id: &str,
    manifest: &UploadSessionManifest,
    part: &PartMetadata,
) -> Result<(), AppError> {
    let total_shards = usize::from(manifest.data_shards) + usize::from(manifest.parity_shards);
    if total_shards == 0 {
        return Ok(());
    }

    if part.shards.len() != total_shards {
        return Err(AppError::InternalServerError(format!(
            "part {} has {} shards, expected {}",
            part.part_index,
            part.shards.len(),
            total_shards
        )));
    }

    let receipt = SessionPartReceipt {
        part_index: part.part_index,
        size_bytes: part.size_bytes,
        shard_size_bytes: part
            .shards
            .first()
            .map(|shard| shard.size_bytes)
            .unwrap_or(0),
        created_at: part.created_at,
        checksum: part.checksum.as_deref().map(parse_crc32_hex).transpose()?,
        shard_checksums: part
            .shards
            .iter()
            .map(|shard| parse_crc32_hex(&shard.checksum))
            .collect::<Result<Vec<_>, _>>()?,
    };
    indexdb_store::commit_session_part_receipt(fragment, version_id, &receipt)
}

pub async fn load_session(
    fragment: &str,
    version_id: &str,
) -> Result<Option<IndexSessionState>, AppError> {
    indexdb_store::load_session(fragment, version_id)
}

pub async fn read_session_part(
    fragment: &str,
    version_id: &str,
    part_index: u64,
) -> Result<Option<PartMetadata>, AppError> {
    indexdb_store::read_session_part(fragment, version_id, part_index)
}

pub async fn load_session_part_receipts(
    fragment: &str,
    version_id: &str,
    manifest: &UploadSessionManifest,
) -> Result<Option<Vec<Option<SessionPartReceipt>>>, AppError> {
    indexdb_store::load_session_part_receipts(fragment, version_id, manifest)
}

pub async fn remove_session(
    fragment: &str,
    version_id: &str,
    _reason: &str,
) -> Result<(), AppError> {
    let _ = indexdb_store::remove_session(fragment, version_id)?;
    Ok(())
}

pub async fn list_active_sessions(fragment: &str) -> Result<Vec<IndexSessionState>, AppError> {
    indexdb_store::list_active_sessions(fragment)
}

pub async fn publish_object(metadata: &ObjectMetadata) -> Result<(), AppError> {
    indexdb_store::upsert_live_version(metadata)
}

pub async fn upsert_object_version(metadata: &ObjectMetadata) -> Result<(), AppError> {
    indexdb_store::upsert_live_version(metadata)
}

pub async fn upsert_trashed_object_version(metadata: &ObjectMetadata) -> Result<(), AppError> {
    indexdb_store::upsert_trash_version(metadata)
}

pub async fn read_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<ObjectMetadata, AppError> {
    indexdb_store::read_live_version(fragment, object_key, version_id)?.ok_or(AppError::NotFound)
}

pub async fn read_trashed_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<ObjectMetadata, AppError> {
    indexdb_store::read_trash_version(fragment, object_key, version_id)?.ok_or(AppError::NotFound)
}

pub async fn list_live_object_versions_metadata(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    indexdb_store::list_live_versions(fragment, object_key)
}

pub async fn list_trashed_object_versions_metadata(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    indexdb_store::list_trash_versions(fragment, object_key)
}

pub async fn list_live_object_version_ids(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<String>, AppError> {
    Ok(indexdb_store::list_live_versions(fragment, object_key)?
        .into_iter()
        .map(|metadata| metadata.version_id)
        .collect())
}

pub async fn list_trashed_object_version_ids(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<String>, AppError> {
    Ok(indexdb_store::list_trash_versions(fragment, object_key)?
        .into_iter()
        .map(|metadata| metadata.version_id)
        .collect())
}

pub async fn read_latest_object(
    fragment: &str,
    object_key: &str,
) -> Result<ObjectMetadata, AppError> {
    indexdb_store::read_current_live_head(fragment, object_key)?.ok_or(AppError::NotFound)
}

pub async fn read_latest_trashed_object(
    fragment: &str,
    object_key: &str,
) -> Result<ObjectMetadata, AppError> {
    indexdb_store::read_latest_trash(fragment, object_key)?.ok_or(AppError::NotFound)
}

pub async fn list_fragment_trash_objects(fragment: &str) -> Result<Vec<ObjectMetadata>, AppError> {
    indexdb_store::list_trash_heads(fragment)
}

pub async fn list_fragment_object_keys_with_trash_versions(
    fragment: &str,
) -> Result<Vec<String>, AppError> {
    indexdb_store::list_object_keys_with_trash_versions(fragment)
}

pub async fn list_active_trash_partition_ids(
    fragment: &str,
) -> Result<std::collections::BTreeSet<u16>, AppError> {
    indexdb_store::list_active_trash_partition_ids(fragment)
}

pub async fn list_partition_trash_entries(
    fragment: &str,
    partition_id: u16,
) -> Result<std::collections::HashMap<String, Vec<String>>, AppError> {
    indexdb_store::list_partition_trash_entries(fragment, partition_id)
}

pub async fn list_fragment_trash_catalog(
    fragment: &str,
) -> Result<Vec<TrashCatalogEntry>, AppError> {
    indexdb_store::list_trash_catalog(fragment)
}

pub async fn list_trashed_object_version_ids_batch(
    fragment: &str,
    object_keys: &[String],
) -> Result<std::collections::HashMap<String, Vec<String>>, AppError> {
    indexdb_store::list_trashed_object_version_ids_batch(fragment, object_keys)
}

pub async fn object_partition_id(fragment: &str, object_key: &str) -> Result<u16, AppError> {
    indexdb_store::object_partition_id(fragment, object_key)
}

pub async fn list_fragment_live_objects(fragment: &str) -> Result<Vec<ObjectMetadata>, AppError> {
    indexdb_store::list_live_heads(fragment)
}

pub async fn scan_fragment_live_objects_page(
    fragment: &str,
    prefix: Option<String>,
    delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    indexdb_store::scan_live_heads_page(
        fragment,
        prefix,
        delimiter,
        limit,
        continuation_token,
        include_internal,
    )
}

pub async fn scan_fragment_live_metadata_page(
    fragment: &str,
    prefix: Option<String>,
    delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    indexdb_store::scan_live_full_metadata_page(
        fragment,
        prefix,
        delimiter,
        limit,
        continuation_token,
        include_internal,
    )
}

pub async fn scan_fragment_trash_objects_page(
    fragment: &str,
    prefix: Option<String>,
    delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    indexdb_store::scan_trash_heads_page(
        fragment,
        prefix,
        delimiter,
        limit,
        continuation_token,
        include_internal,
    )
}

pub async fn scan_fragment_trash_metadata_page(
    fragment: &str,
    prefix: Option<String>,
    delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    indexdb_store::scan_trash_full_metadata_page(
        fragment,
        prefix,
        delimiter,
        limit,
        continuation_token,
        include_internal,
    )
}

pub async fn move_object_version_to_trash_with_metadata(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<ObjectMetadata, AppError> {
    indexdb_store::move_object_version_to_trash_with_metadata(fragment, object_key, version_id)?
        .ok_or(AppError::NotFound)
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
    indexdb_store::restore_object_version_from_trash_with_metadata(
        fragment, object_key, version_id,
    )?
    .ok_or(AppError::NotFound)
}

pub async fn move_object_to_trash_fast(
    fragment: &str,
    object_key: &str,
) -> Result<Option<Vec<String>>, AppError> {
    indexdb_store::move_object_to_trash_fast(fragment, object_key)
}

pub async fn move_objects_to_trash_batch(
    fragment: &str,
    object_keys: &[String],
) -> Result<HashMap<String, Vec<String>>, AppError> {
    indexdb_store::move_objects_to_trash_batch(fragment, object_keys)
}

pub async fn restore_object_from_trash_fast(
    fragment: &str,
    object_key: &str,
) -> Result<Option<Vec<String>>, AppError> {
    indexdb_store::restore_object_from_trash_fast(fragment, object_key)
}

pub async fn restore_objects_from_trash_batch(
    fragment: &str,
    object_keys: &[String],
) -> Result<HashMap<String, Vec<String>>, AppError> {
    indexdb_store::restore_objects_from_trash_batch(fragment, object_keys)
}

pub async fn restore_objects_from_trash_manifest_batch(
    fragment: &str,
    object_versions: &HashMap<String, Vec<String>>,
) -> Result<HashMap<String, Vec<String>>, AppError> {
    indexdb_store::restore_objects_from_trash_manifest_batch(fragment, object_versions)
}

pub async fn delete_all_trash_versions_for_key(
    fragment: &str,
    object_key: &str,
) -> Result<Option<Vec<String>>, AppError> {
    indexdb_store::delete_all_trash_versions_for_key(fragment, object_key)
}

pub async fn delete_object_fast(
    fragment: &str,
    object_key: &str,
) -> Result<Option<Vec<String>>, AppError> {
    indexdb_store::delete_object_fast(fragment, object_key)
}

pub async fn sync_fragment_partition_replicas(fragment: &str) -> Result<(), AppError> {
    indexdb_store::sync_fragment_partition_replicas(fragment)
}

pub async fn ensure_object_owner_partition_available(
    fragment: &str,
    object_key: &str,
) -> Result<(), AppError> {
    indexdb_store::ensure_object_owner_partition_available(fragment, object_key)
}

pub async fn delete_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<(), AppError> {
    if indexdb_store::delete_live_version(fragment, object_key, version_id)? {
        Ok(())
    } else {
        Err(AppError::NotFound)
    }
}

pub async fn delete_trashed_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<(), AppError> {
    if indexdb_store::delete_trash_version(fragment, object_key, version_id)? {
        Ok(())
    } else {
        Err(AppError::NotFound)
    }
}

pub async fn clear_fragment_trash_in_partitions(
    fragment: &str,
    object_key: Option<&str>,
) -> Result<(), AppError> {
    indexdb_store::clear_fragment_trash(fragment, object_key)
}

pub async fn clear_fragment_trash_for_partitions(
    fragment: &str,
    touched_partitions: &std::collections::BTreeSet<u16>,
) -> Result<usize, AppError> {
    indexdb_store::clear_fragment_trash_for_partitions(fragment, touched_partitions)
}

pub async fn clear_fragment_trash_partition(
    fragment: &str,
    partition_id: u16,
) -> Result<usize, AppError> {
    indexdb_store::clear_fragment_trash_partition(fragment, partition_id)
}
