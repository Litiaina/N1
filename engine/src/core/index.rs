use crate::core::checksum::{Sha256Digest, parse_sha256_hex};
use crate::core::disk_manager::{
    IndexListEntry, ListObjectsResult, ObjectMetadata, ObjectPathManager, PartMetadata,
    ReservationReceipt,
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
    pub checksum: Option<Sha256Digest>,
    pub shard_checksums: Vec<Sha256Digest>,
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

fn parse_sha256(value: &str) -> Result<Sha256Digest, AppError> {
    parse_sha256_hex(value)
        .ok_or_else(|| AppError::InternalServerError(format!("invalid SHA-256 value '{value}'")))
}

async fn run_index_blocking<T>(
    op: impl FnOnce() -> Result<T, AppError> + Send + 'static,
) -> Result<T, AppError>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(op)
        .await
        .map_err(|err| AppError::InternalServerError(format!("N2 index worker failed: {err}")))?
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
        checksum: part.checksum.as_deref().map(parse_sha256).transpose()?,
        shard_checksums: part
            .shards
            .iter()
            .map(|shard| parse_sha256(&shard.checksum))
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
    run_index_blocking(move || index_store::upsert_live_version(&metadata)).await
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
    crate::distribution::peer::resolve_cluster_object_version(fragment, object_key, version_id)
        .await?
        .ok_or(AppError::NotFound)
}

pub async fn find_live_object_version_by_id(
    fragment: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let fragment = fragment.to_string();
    let version_id = version_id.to_string();
    run_index_blocking(move || index_store::find_live_version_by_id(&fragment, &version_id)).await
}

pub async fn read_trashed_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<ObjectMetadata, AppError> {
    read_object_version(fragment, object_key, version_id).await
}

pub async fn list_live_object_versions_metadata(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    crate::distribution::peer::resolve_cluster_object_versions(fragment, object_key).await
}

pub async fn list_trashed_object_versions_metadata(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    list_live_object_versions_metadata(fragment, object_key).await
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
    crate::distribution::peer::resolve_cluster_object_versions(fragment, object_key)
        .await?
        .into_iter()
        .max_by(|left, right| left.version_id.cmp(&right.version_id))
        .ok_or(AppError::NotFound)
}

pub async fn read_latest_trashed_object(
    fragment: &str,
    object_key: &str,
) -> Result<ObjectMetadata, AppError> {
    read_latest_object(fragment, object_key).await
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

pub async fn list_fragment_live_objects(fragment: &str) -> Result<Vec<ObjectMetadata>, AppError> {
    let fragment = fragment.to_string();
    run_index_blocking(move || index_store::list_live_heads(&fragment)).await
}

pub async fn list_fragment_local_committed_heads(
    fragment: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let fragment = fragment.to_string();
    run_index_blocking(move || index_store::list_local_committed_heads(&fragment)).await
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
    scan_fragment_metadata_page(
        fragment,
        prefix,
        delimiter,
        limit,
        continuation_token,
        include_internal,
        false,
    )
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
    scan_fragment_metadata_page(
        fragment,
        prefix,
        delimiter,
        limit,
        continuation_token,
        include_internal,
        true,
    )
    .await
}

async fn scan_fragment_metadata_page(
    fragment: &str,
    prefix: Option<String>,
    _delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    _include_internal: bool,
    include_trashed: bool,
) -> Result<ListObjectsResult, AppError> {
    let refs = if include_trashed {
        crate::namespace::runtime::global_namespace_runtime()
            .list_trash_object_refs_page(
                fragment,
                prefix.as_deref(),
                continuation_token.as_deref(),
                limit,
            )
            .await?
    } else {
        crate::namespace::runtime::global_namespace_runtime()
            .list_live_object_refs_page(
                fragment,
                prefix.as_deref(),
                continuation_token.as_deref(),
                limit,
            )
            .await?
    };
    let object_ids = refs
        .entries
        .iter()
        .map(|entry| entry.object_id.clone())
        .collect::<Vec<_>>();
    let heads =
        crate::distribution::peer::resolve_cluster_object_heads(fragment, &object_ids).await?;
    let mut entries = Vec::with_capacity(refs.entries.len());
    let mut objects = Vec::with_capacity(refs.entries.len());
    for object_ref in refs.entries {
        let Some(metadata) = heads.get(&object_ref.object_id).cloned() else {
            continue;
        };
        entries.push(IndexListEntry {
            record: object_ref.path,
            kind: "object".to_string(),
            size_bytes: metadata.total_size_bytes,
            created_at_unix_nanos: metadata.created_at.saturating_mul(1_000_000_000),
            packed_ext_u64: Some(metadata.packed_ext_u64),
            version_ulid: Some(metadata.version_id.clone()),
        });
        objects.push(metadata);
    }
    Ok(ListObjectsResult {
        prefix,
        limit: Some(limit),
        entries,
        objects,
        common_prefixes: Vec::new(),
        continuation_token: refs.continuation_token.clone(),
        next_continuation_token: refs.continuation_token,
    })
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

pub async fn delete_all_trash_versions_for_key(
    fragment: &str,
    object_key: &str,
) -> Result<Option<Vec<String>>, AppError> {
    let fragment = fragment.to_string();
    let object_key = object_key.to_string();
    run_index_blocking(move || {
        index_store::delete_all_trash_versions_for_key(&fragment, &object_key)
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

pub async fn delete_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<(), AppError> {
    let fragment = fragment.to_string();
    let object_key = object_key.to_string();
    let version_id = version_id.to_string();
    run_index_blocking(move || {
        if index_store::delete_live_version(&fragment, &object_key, &version_id)? {
            Ok(())
        } else {
            Err(AppError::NotFound)
        }
    })
    .await
}

pub async fn delete_trashed_object_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<(), AppError> {
    let fragment = fragment.to_string();
    let object_key = object_key.to_string();
    let version_id = version_id.to_string();
    run_index_blocking(move || {
        if index_store::delete_trash_version(&fragment, &object_key, &version_id)? {
            Ok(())
        } else {
            Err(AppError::NotFound)
        }
    })
    .await
}

pub async fn clear_fragment_trash_in_partitions(
    fragment: &str,
    object_key: Option<&str>,
) -> Result<(), AppError> {
    let fragment = fragment.to_string();
    let object_key = object_key.map(ToOwned::to_owned);
    run_index_blocking(move || index_store::clear_fragment_trash(&fragment, object_key.as_deref()))
        .await
}
