use crate::core::disk_manager::{
    ListObjectsResult, ObjectMetadata, ObjectPathManager, PartMetadata, ReservationReceipt,
};
use crate::core::index_store;
use crate::core::upload::UploadSessionManifest;
use crate::enums::app_error_enum::AppError;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, LazyLock, Mutex};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

static OBJECT_OP_LOCKS: LazyLock<Mutex<HashMap<String, Arc<AsyncMutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexWriteDurability {
    Relaxed,
    Strict,
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
    key: String,
    lock: Arc<AsyncMutex<()>>,
    guard: Option<OwnedMutexGuard<()>>,
}

impl Drop for ObjectOperationGuard {
    fn drop(&mut self) {
        drop(self.guard.take());
        let Ok(mut locks) = OBJECT_OP_LOCKS.lock() else {
            return;
        };
        if locks.get(&self.key).is_some_and(|current| {
            Arc::ptr_eq(current, &self.lock) && Arc::strong_count(current) <= 2
        }) {
            locks.remove(&self.key);
        }
    }
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
            .entry(key.clone())
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    };
    let guard = lock.clone().lock_owned().await;
    Ok(ObjectOperationGuard {
        key,
        lock,
        guard: Some(guard),
    })
}

#[cfg(test)]
fn object_operation_lock_count_for_fragment(fragment: &str) -> usize {
    let prefix = format!("{fragment}\u{001f}");
    OBJECT_OP_LOCKS
        .lock()
        .map(|locks| locks.keys().filter(|key| key.starts_with(&prefix)).count())
        .unwrap_or(0)
}

fn parse_crc32_hex(value: &str) -> Result<u32, AppError> {
    u32::from_str_radix(value.trim(), 16).map_err(|e| {
        AppError::InternalServerError(format!("invalid crc32 value '{}': {}", value, e))
    })
}

async fn run_index_blocking<T>(
    op: impl FnOnce() -> Result<T, AppError> + Send + 'static,
) -> Result<T, AppError>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(op)
        .await
        .map_err(|err| AppError::InternalServerError(format!("N1 index worker failed: {err}")))?
}

pub(crate) struct MetadataScan {
    state: Option<index_store::MetadataScan>,
}

impl MetadataScan {
    pub(crate) async fn new(fragment: &str, trash: bool) -> Result<Self, AppError> {
        let fragment = fragment.to_string();
        let state =
            run_index_blocking(move || index_store::MetadataScan::new(&fragment, trash)).await?;
        Ok(Self { state: Some(state) })
    }

    pub(crate) async fn next_batch(
        &mut self,
        limit: usize,
    ) -> Result<Vec<ObjectMetadata>, AppError> {
        let mut state = self.state.take().ok_or_else(|| {
            AppError::InternalServerError("metadata cursor worker did not return".into())
        })?;
        let (state, batch) = run_index_blocking(move || {
            let batch = state.next_batch(limit);
            Ok((state, batch))
        })
        .await?;
        self.state = Some(state);
        batch
    }
}

#[cfg(test)]
mod metadata_cursor_tests {
    #[test]
    fn paused_cursor_does_not_occupy_a_blocking_worker() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let _guard = crate::test_support::global_test_lock().lock().await;
            let base =
                std::env::temp_dir().join(format!("cursor-worker-test-{}", std::process::id()));
            for (cell, role) in [
                (&crate::core::disk_manager::INDEX_STORE_DISKS, "index"),
                (&crate::core::disk_manager::BLOB_STORE_DISKS, "blob"),
            ] {
                for root in cell.get_or_init(|| {
                    (0..2)
                        .map(|i| base.join(format!("{role}-{i}")).display().to_string())
                        .collect()
                }) {
                    std::fs::create_dir_all(root).unwrap();
                }
            }
            let mut scan =
                super::MetadataScan::new(&format!("empty-cursor-{}", ulid::Ulid::new()), false)
                    .await
                    .unwrap();
            assert!(scan.next_batch(1).await.unwrap().is_empty());
            assert!(
                tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    tokio::task::spawn_blocking(|| true)
                )
                .await
                .unwrap()
                .unwrap()
            );
            assert!(scan.next_batch(1).await.unwrap().is_empty());
        });
    }
}

pub async fn open_session(
    manifest: &UploadSessionManifest,
    reservation: Option<ReservationReceipt>,
) -> Result<(), AppError> {
    let manifest = manifest.clone();
    run_index_blocking(move || index_store::open_session(&manifest, reservation)).await
}

pub async fn commit_session_part(
    fragment: &str,
    version_id: &str,
    part: &PartMetadata,
    _write_durability: IndexWriteDurability,
) -> Result<(), AppError> {
    let fragment = fragment.to_string();
    let version_id = version_id.to_string();
    let part = part.clone();
    run_index_blocking(move || index_store::commit_session_part(&fragment, &version_id, &part))
        .await
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
    let fragment = fragment.to_string();
    let version_id = version_id.to_string();
    run_index_blocking(move || {
        index_store::commit_session_part_receipt(&fragment, &version_id, &receipt)
    })
    .await
}

pub async fn load_session(
    fragment: &str,
    version_id: &str,
) -> Result<Option<IndexSessionState>, AppError> {
    let fragment = fragment.to_string();
    let version_id = version_id.to_string();
    run_index_blocking(move || index_store::load_session(&fragment, &version_id)).await
}

pub async fn read_session_part(
    fragment: &str,
    version_id: &str,
    part_index: u64,
) -> Result<Option<PartMetadata>, AppError> {
    let fragment = fragment.to_string();
    let version_id = version_id.to_string();
    run_index_blocking(move || index_store::read_session_part(&fragment, &version_id, part_index))
        .await
}

pub async fn load_session_part_receipts(
    fragment: &str,
    version_id: &str,
    manifest: &UploadSessionManifest,
) -> Result<Option<Vec<Option<SessionPartReceipt>>>, AppError> {
    let fragment = fragment.to_string();
    let version_id = version_id.to_string();
    let manifest = manifest.clone();
    run_index_blocking(move || {
        index_store::load_session_part_receipts(&fragment, &version_id, &manifest)
    })
    .await
}

pub async fn remove_session(
    fragment: &str,
    version_id: &str,
    _reason: &str,
) -> Result<(), AppError> {
    let fragment = fragment.to_string();
    let version_id = version_id.to_string();
    run_index_blocking(move || {
        let _ = index_store::remove_session(&fragment, &version_id)?;
        Ok(())
    })
    .await
}

pub async fn list_active_sessions(fragment: &str) -> Result<Vec<IndexSessionState>, AppError> {
    let fragment = fragment.to_string();
    run_index_blocking(move || index_store::list_active_sessions(&fragment)).await
}

pub async fn publish_object(metadata: &ObjectMetadata) -> Result<(), AppError> {
    let metadata = metadata.clone();
    run_index_blocking(move || {
        super::upload::staging::promote(&metadata, true)?;
        index_store::upsert_live_version(&metadata)?;
        super::upload::staging::retire_empty(&metadata);
        Ok(())
    })
    .await
}

pub async fn publish_object_with_durability(
    metadata: &ObjectMetadata,
    write_durability: IndexWriteDurability,
) -> Result<(), AppError> {
    let metadata = metadata.clone();
    run_index_blocking(move || {
        super::upload::staging::promote(
            &metadata,
            matches!(write_durability, IndexWriteDurability::Strict),
        )?;
        index_store::upsert_live_version_with_durability(&metadata, write_durability)?;
        super::upload::staging::retire_empty(&metadata);
        Ok(())
    })
    .await
}

pub async fn upsert_object_version(metadata: &ObjectMetadata) -> Result<(), AppError> {
    let metadata = metadata.clone();
    run_index_blocking(move || index_store::upsert_live_version(&metadata)).await
}

pub async fn publish_trashed_object_version_for_maintenance(
    metadata: &ObjectMetadata,
) -> Result<(), AppError> {
    let metadata = metadata.clone();
    run_index_blocking(move || index_store::publish_trash_version_for_maintenance(&metadata)).await
}

pub async fn read_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<ObjectMetadata, AppError> {
    let fragment = fragment.to_string();
    let object_key = object_key.to_string();
    let version_id = version_id.to_string();
    run_index_blocking(move || {
        index_store::read_live_version(&fragment, &object_key, &version_id)?
            .ok_or(AppError::NotFound)
    })
    .await
}

pub async fn read_trashed_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<ObjectMetadata, AppError> {
    let fragment = fragment.to_string();
    let object_key = object_key.to_string();
    let version_id = version_id.to_string();
    run_index_blocking(move || {
        index_store::read_trash_version(&fragment, &object_key, &version_id)?
            .ok_or(AppError::NotFound)
    })
    .await
}

pub async fn list_live_object_versions_metadata(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let fragment = fragment.to_string();
    let object_key = object_key.to_string();
    run_index_blocking(move || index_store::list_live_versions(&fragment, &object_key)).await
}

pub async fn list_trashed_object_versions_metadata(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let fragment = fragment.to_string();
    let object_key = object_key.to_string();
    run_index_blocking(move || index_store::list_trash_versions(&fragment, &object_key)).await
}

pub async fn list_live_object_version_ids(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<String>, AppError> {
    let fragment = fragment.to_string();
    let object_key = object_key.to_string();
    run_index_blocking(move || {
        Ok(index_store::list_live_versions(&fragment, &object_key)?
            .into_iter()
            .map(|metadata| metadata.version_id)
            .collect())
    })
    .await
}

pub async fn list_trashed_object_version_ids(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<String>, AppError> {
    let fragment = fragment.to_string();
    let object_key = object_key.to_string();
    run_index_blocking(move || {
        Ok(index_store::list_trash_versions(&fragment, &object_key)?
            .into_iter()
            .map(|metadata| metadata.version_id)
            .collect())
    })
    .await
}

pub async fn read_latest_object(
    fragment: &str,
    object_key: &str,
) -> Result<ObjectMetadata, AppError> {
    let fragment = fragment.to_string();
    let object_key = object_key.to_string();
    run_index_blocking(move || {
        index_store::read_current_live_head(&fragment, &object_key)?.ok_or(AppError::NotFound)
    })
    .await
}

pub async fn read_latest_trashed_object(
    fragment: &str,
    object_key: &str,
) -> Result<ObjectMetadata, AppError> {
    let fragment = fragment.to_string();
    let object_key = object_key.to_string();
    run_index_blocking(move || {
        index_store::read_latest_trash(&fragment, &object_key)?.ok_or(AppError::NotFound)
    })
    .await
}

pub async fn list_fragment_trash_objects(fragment: &str) -> Result<Vec<ObjectMetadata>, AppError> {
    let fragment = fragment.to_string();
    run_index_blocking(move || index_store::list_trash_heads(&fragment)).await
}

pub async fn list_fragment_object_keys_with_trash_versions(
    fragment: &str,
) -> Result<Vec<String>, AppError> {
    let fragment = fragment.to_string();
    run_index_blocking(move || index_store::list_object_keys_with_trash_versions(&fragment)).await
}

pub async fn list_active_trash_partition_ids(
    fragment: &str,
) -> Result<std::collections::BTreeSet<u16>, AppError> {
    let fragment = fragment.to_string();
    run_index_blocking(move || index_store::list_active_trash_partition_ids(&fragment)).await
}

pub async fn list_partition_trash_entries(
    fragment: &str,
    partition_id: u16,
) -> Result<std::collections::HashMap<String, Vec<String>>, AppError> {
    let fragment = fragment.to_string();
    run_index_blocking(move || index_store::list_partition_trash_entries(&fragment, partition_id))
        .await
}

pub async fn list_fragment_trash_catalog(
    fragment: &str,
) -> Result<Vec<TrashCatalogEntry>, AppError> {
    let fragment = fragment.to_string();
    run_index_blocking(move || index_store::list_trash_catalog(&fragment)).await
}

pub async fn list_trashed_object_version_ids_batch(
    fragment: &str,
    object_keys: &[String],
) -> Result<std::collections::HashMap<String, Vec<String>>, AppError> {
    let fragment = fragment.to_string();
    let object_keys = object_keys.to_vec();
    run_index_blocking(move || {
        index_store::list_trashed_object_version_ids_batch(&fragment, &object_keys)
    })
    .await
}

pub async fn object_partition_id(fragment: &str, object_key: &str) -> Result<u16, AppError> {
    let fragment = fragment.to_string();
    let object_key = object_key.to_string();
    run_index_blocking(move || index_store::object_partition_id(&fragment, &object_key)).await
}

pub async fn list_fragment_live_objects(fragment: &str) -> Result<Vec<ObjectMetadata>, AppError> {
    let fragment = fragment.to_string();
    run_index_blocking(move || index_store::list_live_heads(&fragment)).await
}

pub async fn scan_fragment_live_objects_page(
    fragment: &str,
    prefix: Option<String>,
    delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    let fragment = fragment.to_string();
    run_index_blocking(move || {
        index_store::scan_live_heads_page(
            &fragment,
            prefix,
            delimiter,
            limit,
            continuation_token,
            include_internal,
        )
    })
    .await
}

pub async fn scan_fragment_live_metadata_page(
    fragment: &str,
    prefix: Option<String>,
    delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    let fragment = fragment.to_string();
    run_index_blocking(move || {
        index_store::scan_live_full_metadata_page(
            &fragment,
            prefix,
            delimiter,
            limit,
            continuation_token,
            include_internal,
        )
    })
    .await
}

pub async fn scan_fragment_trash_objects_page(
    fragment: &str,
    prefix: Option<String>,
    delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    let fragment = fragment.to_string();
    run_index_blocking(move || {
        index_store::scan_trash_heads_page(
            &fragment,
            prefix,
            delimiter,
            limit,
            continuation_token,
            include_internal,
        )
    })
    .await
}

pub async fn scan_fragment_trash_metadata_page(
    fragment: &str,
    prefix: Option<String>,
    delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    let fragment = fragment.to_string();
    run_index_blocking(move || {
        index_store::scan_trash_full_metadata_page(
            &fragment,
            prefix,
            delimiter,
            limit,
            continuation_token,
            include_internal,
        )
    })
    .await
}

pub async fn move_object_version_to_trash_with_metadata(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<ObjectMetadata, AppError> {
    let fragment = fragment.to_string();
    let object_key = object_key.to_string();
    let version_id = version_id.to_string();
    run_index_blocking(move || {
        index_store::move_object_version_to_trash_with_metadata(
            &fragment,
            &object_key,
            &version_id,
        )?
        .ok_or(AppError::NotFound)
    })
    .await
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
    let fragment = fragment.to_string();
    let object_key = object_key.to_string();
    let version_id = version_id.to_string();
    run_index_blocking(move || {
        index_store::restore_object_version_from_trash_with_metadata(
            &fragment,
            &object_key,
            &version_id,
        )?
        .ok_or(AppError::NotFound)
    })
    .await
}

pub async fn move_object_to_trash_fast(
    fragment: &str,
    object_key: &str,
) -> Result<Option<Vec<String>>, AppError> {
    let fragment = fragment.to_string();
    let object_key = object_key.to_string();
    run_index_blocking(move || index_store::move_object_to_trash_fast(&fragment, &object_key)).await
}

pub async fn move_objects_to_trash_batch(
    fragment: &str,
    object_keys: &[String],
) -> Result<HashMap<String, Vec<String>>, AppError> {
    let fragment = fragment.to_string();
    let object_keys = object_keys.to_vec();
    run_index_blocking(move || index_store::move_objects_to_trash_batch(&fragment, &object_keys))
        .await
}

pub async fn restore_object_from_trash_fast(
    fragment: &str,
    object_key: &str,
) -> Result<Option<Vec<String>>, AppError> {
    let fragment = fragment.to_string();
    let object_key = object_key.to_string();
    run_index_blocking(move || index_store::restore_object_from_trash_fast(&fragment, &object_key))
        .await
}

pub async fn restore_objects_from_trash_batch(
    fragment: &str,
    object_keys: &[String],
) -> Result<HashMap<String, Vec<String>>, AppError> {
    let fragment = fragment.to_string();
    let object_keys = object_keys.to_vec();
    run_index_blocking(move || {
        index_store::restore_objects_from_trash_batch(&fragment, &object_keys)
    })
    .await
}

pub async fn restore_objects_from_trash_manifest_batch(
    fragment: &str,
    object_versions: &HashMap<String, Vec<String>>,
) -> Result<HashMap<String, Vec<String>>, AppError> {
    let fragment = fragment.to_string();
    let object_versions = object_versions.clone();
    run_index_blocking(move || {
        index_store::restore_objects_from_trash_manifest_batch(&fragment, &object_versions)
    })
    .await
}

pub async fn delete_object_fast(
    fragment: &str,
    object_key: &str,
) -> Result<Option<Vec<String>>, AppError> {
    let fragment = fragment.to_string();
    let object_key = object_key.to_string();
    run_index_blocking(move || index_store::delete_object_fast(&fragment, &object_key)).await
}

pub async fn sync_fragment_partition_replicas(fragment: &str) -> Result<(), AppError> {
    let fragment = fragment.to_string();
    run_index_blocking(move || index_store::sync_fragment_partition_replicas(&fragment)).await
}

pub async fn ensure_object_owner_partition_available(
    fragment: &str,
    object_key: &str,
) -> Result<(), AppError> {
    let fragment = fragment.to_string();
    let object_key = object_key.to_string();
    run_index_blocking(move || {
        index_store::ensure_object_owner_partition_available(&fragment, &object_key)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn object_operation_registry_is_bounded_and_same_key_is_serialized() {
        let fragment = format!("lock-lifetime-{}", ulid::Ulid::new());
        for index in 0..4_096usize {
            let guard = acquire_object_operation_lock(&fragment, &format!("object-{index}"))
                .await
                .expect("acquire unique object operation guard");
            drop(guard);
        }
        assert_eq!(object_operation_lock_count_for_fragment(&fragment), 0);

        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..128usize {
            let fragment = fragment.clone();
            let active = active.clone();
            let max_active = max_active.clone();
            tasks.push(tokio::spawn(async move {
                let _guard = acquire_object_operation_lock(&fragment, "shared")
                    .await
                    .expect("acquire shared object operation guard");
                let now = active.fetch_add(1, Ordering::AcqRel) + 1;
                max_active.fetch_max(now, Ordering::AcqRel);
                tokio::task::yield_now().await;
                active.fetch_sub(1, Ordering::AcqRel);
            }));
        }
        for task in tasks {
            task.await.expect("object operation task");
        }
        assert_eq!(max_active.load(Ordering::Acquire), 1);
        assert_eq!(object_operation_lock_count_for_fragment(&fragment), 0);
    }
}
