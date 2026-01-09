use std::sync::Arc;

use crate::enums::app_error_enum::AppError;
use crate::object_storage::caching::invalidate_cache;
use crate::object_storage::constants::OBJECT_DATA_ROOT;
use crate::object_storage::quota_manager::update_quota;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{ObjectMetadata, ObjectPathManager};
use crate::websocket::ws_structs::StorageEventPayload;
use axum::Json;
use axum::extract::State;
use axum::{extract::Path, http::StatusCode, response::IntoResponse};
use chrono::Utc;
use futures_util::{StreamExt, stream};
use sha2::{Digest, Sha256};
use tokio::fs;
use tracing::{error, info, warn};
use ulid::Ulid;
use walkdir::WalkDir;

pub async fn delete_object_version(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((fragment, version_id, object_key)): Path<(String, String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let path_manager = ObjectPathManager::new();

    let index_path = path_manager.get_metadata_path(&uid, &fragment, &object_key, &version_id);
    let meta_dir = path_manager.get_blob_dir(&uid, &fragment, &version_id);

    let mut size_to_free = 0;
    if let Ok(data) = fs::read(&index_path).await {
        if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
            if !meta.is_delete_marker {
                size_to_free = meta.total_size_bytes as i64;
            }
        }
    }

    fs::remove_file(&index_path).await.map_err(|e| {
        error!("Failed to delete version metadata: {}", e);
        AppError::InternalServerError("Failed to delete version metadata.".to_string())
    })?;

    if let Some(shard_h2) = index_path.parent() {
        if fs::remove_dir(shard_h2).await.is_ok() {
            if let Some(shard_h1) = shard_h2.parent() {
                let _ = fs::remove_dir(shard_h1).await;
            }
        }
    }

    if fs::try_exists(&meta_dir).await.unwrap_or(false) {
        let _ = fs::remove_dir_all(&meta_dir).await;
    }

    let blob_roots = path_manager.get_blob_roots();
    for disk in blob_roots {
        let shard_dir = path_manager.get_shard_dir_on_disk(&disk, &uid, &fragment, &version_id);
        if shard_dir.exists() {
            if let Err(e) = fs::remove_dir_all(&shard_dir).await {
                warn!("Failed to delete blob dir on disk {:?}: {}", shard_dir, e);
            }
        }
    }

    if size_to_free > 0 {
        if let Err(e) = update_quota(&uid, -size_to_free).await {
            error!("Failed to refund quota for user {}: {}", uid, e);
        }
    }

    invalidate_cache(&fragment, &object_key, &uid).await;

    info!(
        "Hard deleted version: {}/{} (v: {})",
        fragment, object_key, version_id
    );

    state.emit(
        &uid,
        StorageEventPayload::FileDeleted {
            fragment,
            object_key,
            version_id: Some(version_id),
        },
    );

    Ok(StatusCode::NO_CONTENT)
}

pub async fn delete_object_permanently(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let path_manager = ObjectPathManager::new();

    let mut hasher = Sha256::new();
    hasher.update(object_key.as_bytes());
    let hash = hex::encode(hasher.finalize());

    let shard1 = &hash[0..2];
    let shard2 = &hash[2..4];

    let shard_dir = path_manager
        .get_index_dir(&uid, &fragment)
        .join(shard1)
        .join(shard2);

    if !fs::try_exists(&shard_dir).await.unwrap_or(false) {
        return Err(AppError::NotFound);
    }

    let prefix = format!("{}#", hash);
    let mut version_metadata_files = Vec::new();

    let mut entries = fs::read_dir(&shard_dir)
        .await
        .map_err(|e| AppError::Io(e, "read index shard".into()))?;

    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with(&prefix) && name.ends_with(".bson") {
            version_metadata_files.push(entry.path());
        }
    }

    let blob_roots = Arc::new(path_manager.get_blob_roots());

    let results: Vec<i64> = stream::iter(version_metadata_files)
        .map(|meta_path| {
            let pm = ObjectPathManager::new();
            let u = uid.clone();
            let f = fragment.clone();
            let disks = blob_roots.clone();

            async move {
                let mut freed_bytes = 0;
                let mut version_id = String::new();

                if let Ok(data) = fs::read(&meta_path).await {
                    if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                        version_id = meta.version_id.clone();
                        if !meta.is_delete_marker {
                            freed_bytes = meta.total_size_bytes as i64;
                        }
                    }
                }

                if let Err(e) = fs::remove_file(&meta_path).await {
                    error!("Failed to delete index file {:?}: {}", meta_path, e);
                    return 0;
                }

                if !version_id.is_empty() {
                    let meta_dir = pm.get_blob_dir(&u, &f, &version_id);
                    let _ = fs::remove_dir_all(&meta_dir).await;

                    for disk in disks.iter() {
                        let shard_dir = pm.get_shard_dir_on_disk(disk, &u, &f, &version_id);
                        let _ = fs::remove_dir_all(&shard_dir).await;
                    }
                }

                freed_bytes
            }
        })
        .buffer_unordered(20)
        .collect()
        .await;

    if fs::remove_dir(&shard_dir).await.is_ok() {
        if let Some(parent) = shard_dir.parent() {
            let _ = fs::remove_dir(parent).await;
        }
    }

    let total_size_to_free: i64 = results.iter().sum();

    if total_size_to_free > 0 {
        if let Err(e) = update_quota(&uid, -total_size_to_free).await {
            error!("Failed to refund quota for user {}: {}", uid, e);
        }
    }

    invalidate_cache(&fragment, &object_key, &uid).await;

    info!(
        "Permanently deleted object versions: {}/{} (Freed: {} bytes)",
        fragment, object_key, total_size_to_free
    );

    state.emit(
        &uid,
        StorageEventPayload::FileDeleted {
            fragment,
            object_key,
            version_id: None,
        },
    );

    Ok(StatusCode::NO_CONTENT)
}

pub async fn delete_object(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let version_id = Ulid::new().to_string();
    let session_id = Ulid::new().to_string();

    let path_manager = ObjectPathManager::new();

    let index_path = path_manager.get_metadata_path(&uid, &fragment, &object_key, &version_id);

    if let Some(parent) = index_path.parent() {
        if !fs::try_exists(parent).await.unwrap_or(false) {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| AppError::Io(e, "create index shard".into()))?;
        }
    }

    let delete_marker = ObjectMetadata {
        object_key: object_key.clone(),
        fragment: fragment.clone(),
        version_id: version_id.clone(),
        upload_session_id: session_id,
        owner_uid: uid.clone(),
        content_type: "application/x-delete-marker".to_string(),
        total_size_bytes: 0,
        created_at: Utc::now().timestamp(),
        parts: vec![],
        is_delete_marker: true,
    };

    let bson_data = mongodb::bson::to_vec(&delete_marker)
        .map_err(|_| AppError::InternalServerError("BSON error".into()))?;

    fs::write(&index_path, bson_data)
        .await
        .map_err(|e| AppError::Io(e, "write marker".into()))?;

    invalidate_cache(&fragment, &object_key, &uid).await;

    info!("Created delete marker: {}/{}", fragment, object_key);

    state.emit(
        &uid,
        StorageEventPayload::FileDeleted {
            fragment,
            object_key,
            version_id: None,
        },
    );

    Ok(StatusCode::NO_CONTENT)
}

pub async fn delete_fragment(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path(fragment): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let path_manager = ObjectPathManager::new();

    let user_storage_root = path_manager.get_user_index_dir(&uid);
    let fragment_path = user_storage_root.join(&fragment);

    let index_dir = fragment_path.join("index");

    let refund_amount = tokio::task::spawn_blocking(move || -> i64 {
        if !index_dir.exists() {
            return 0;
        }

        let mut total_size: u64 = 0;
        for entry in WalkDir::new(&index_dir).into_iter().filter_map(|e| e.ok()) {
            if entry.file_type().is_file() && entry.file_name().to_string_lossy().ends_with(".bson")
            {
                if let Ok(data) = std::fs::read(entry.path()) {
                    if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                        if !meta.is_delete_marker {
                            total_size += meta.total_size_bytes;
                        }
                    }
                }
            }
        }
        total_size as i64
    })
    .await
    .unwrap_or(0);

    let blob_roots = Arc::new(path_manager.get_blob_roots());
    let uid_owned = uid.clone();
    let frag_owned = fragment.clone();

    tokio::task::spawn_blocking(move || -> Result<(), AppError> {
        if !fragment_path.exists() {
            return Err(AppError::NotFound);
        }

        std::fs::remove_dir_all(&fragment_path).map_err(|e| {
            error!("Failed to delete fragment index {:?}: {}", fragment_path, e);
            AppError::InternalServerError("Failed to delete fragment index.".to_string())
        })?;

        for disk in blob_roots.iter() {
            let blob_frag_dir = disk
                .join(OBJECT_DATA_ROOT)
                .join(&uid_owned)
                .join(&frag_owned);
            if blob_frag_dir.exists() {
                let _ = std::fs::remove_dir_all(&blob_frag_dir);
            }
        }

        Ok(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("Task join error: {}", e)))??;

    info!("Deleted fragment '{}' for user {}", fragment, uid);

    if refund_amount > 0 {
        if let Err(e) = update_quota(&uid, -refund_amount).await {
            tracing::warn!(
                "Failed to refund quota after fragment delete for {}: {}",
                uid,
                e
            );
        } else {
            info!(
                "Refunded {} bytes to user {} after fragment delete",
                refund_amount, uid
            );
        }
    }

    state.emit(
        &uid,
        StorageEventPayload::FragmentDeleted { fragment: fragment },
    );

    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "success",
            "message": "Fragment deleted successfully"
        })),
    ))
}
