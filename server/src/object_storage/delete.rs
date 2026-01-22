use std::sync::Arc;

use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::OBJECT_DATA_ROOT;
use crate::object_storage::directory_index::DirectoryIndexManager;
use crate::object_storage::quota_manager::update_quota;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{ObjectMetadata, ObjectPathManager};
use crate::websocket::ws_structs::StorageEventPayload;
use axum::Json;
use axum::extract::State;
use axum::{extract::Path, http::StatusCode, response::IntoResponse};
use chrono::Utc;
use futures_util::future::join_all;
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

    let index_dirs = path_manager.get_all_metadata_paths(&uid, &fragment, &object_key, &version_id);
    let mut size_to_free = 0;

    for index_path in &index_dirs {
        if let Ok(data) = fs::read(index_path).await {
            if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                if !meta.is_delete_marker {
                    size_to_free = meta.total_size_bytes as i64;
                    break;
                }
            }
        }
    }

    let index_delete_futures = index_dirs.iter().map(|path| async move {
        if let Err(e) = fs::remove_file(path).await {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!("Failed to delete version metadata at {:?}: {}", path, e);
            }
        } else {
            if let Some(shard_h2) = path.parent() {
                if fs::remove_dir(shard_h2).await.is_ok() {
                    if let Some(shard_h1) = shard_h2.parent() {
                        let _ = fs::remove_dir(shard_h1).await;
                    }
                }
            }
        }
    });

    join_all(index_delete_futures).await;

    let session_dirs = path_manager.get_all_session_dirs(&uid, &fragment, &version_id);
    let session_delete_futures = session_dirs.iter().map(|dir| async move {
        if fs::try_exists(dir).await.unwrap_or(false) {
            let _ = fs::remove_dir_all(dir).await;
        }
    });

    join_all(session_delete_futures).await;

    let blob_roots = path_manager.get_blob_roots();
    let blob_delete_futures = blob_roots.iter().map(|disk| {
        let shard_dir = path_manager.get_shard_dir_on_disk(disk, &uid, &fragment, &version_id);
        async move {
            if shard_dir.exists() {
                if let Err(e) = fs::remove_dir_all(&shard_dir).await {
                    warn!("Failed to delete blob dir on disk {:?}: {}", shard_dir, e);
                }
            }
        }
    });

    join_all(blob_delete_futures).await;

    if size_to_free > 0 {
        if let Err(e) = update_quota(&uid, -size_to_free).await {
            error!("Failed to refund quota for user {}: {}", uid, e);
        }
    }

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
    let index_dirs = path_manager.get_all_index_dirs(&uid, &fragment);

    let mut active_shard_dir = None;
    for dir in index_dirs {
        let candidate = dir.join(shard1).join(shard2);
        if fs::try_exists(&candidate).await.unwrap_or(false) {
            active_shard_dir = Some(candidate);
            break;
        }
    }

    let shard_dir = active_shard_dir.ok_or(AppError::NotFound)?;
    let prefix = format!("{}#", hash);

    let mut versions_to_delete = Vec::new();

    let mut entries = fs::read_dir(&shard_dir)
        .await
        .map_err(|e| AppError::Io(e, "read index shard".into()))?;

    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with(&prefix) && name.ends_with(".bson") {
            if let Ok(data) = fs::read(entry.path()).await {
                if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                    let size = if !meta.is_delete_marker {
                        meta.total_size_bytes as i64
                    } else {
                        0
                    };
                    versions_to_delete.push((meta.version_id, size));
                }
            }
        }
    }

    let blob_roots = Arc::new(path_manager.get_blob_roots());
    let path_manager = Arc::new(path_manager);

    let results: Vec<i64> = stream::iter(versions_to_delete)
        .map(|(version_id, size)| {
            let u = uid.clone();
            let f = fragment.clone();
            let k = object_key.clone();
            let pm = path_manager.clone();
            let disks = blob_roots.clone();

            async move {
                let meta_paths = pm.get_all_metadata_paths(&u, &f, &k, &version_id);
                for mp in meta_paths {
                    let _ = fs::remove_file(&mp).await;
                }

                let session_dirs = pm.get_all_session_dirs(&u, &f, &version_id);
                for sd in session_dirs {
                    let _ = fs::remove_dir_all(&sd).await;
                }

                for disk in disks.iter() {
                    let shard_dir = pm.get_shard_dir_on_disk(disk, &u, &f, &version_id);
                    let _ = fs::remove_dir_all(&shard_dir).await;
                }

                size
            }
        })
        .buffer_unordered(20)
        .collect()
        .await;

    let all_index_dirs = path_manager.get_all_index_dirs(&uid, &fragment);
    for dir in all_index_dirs {
        let s2_dir = dir.join(shard1).join(shard2);
        if fs::remove_dir(&s2_dir).await.is_ok() {
            if let Some(s1_dir) = s2_dir.parent() {
                let _ = fs::remove_dir(s1_dir).await;
            }
        }
    }

    let total_size_to_free: i64 = results.iter().sum();

    if total_size_to_free > 0 {
        if let Err(e) = update_quota(&uid, -total_size_to_free).await {
            error!("Failed to refund quota for user {}: {}", uid, e);
        }
    }

    info!(
        "Permanently deleted object versions: {}/{} (Freed: {} bytes)",
        fragment, object_key, total_size_to_free
    );

    state.emit(
        &uid,
        StorageEventPayload::FileDeleted {
            fragment: fragment.clone(),
            object_key: object_key.clone(),
            version_id: None,
        },
    );

    DirectoryIndexManager::trigger_background_removal(uid, fragment, object_key);

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
        stripe_size_bytes: 0,
    };

    let bson_data = mongodb::bson::to_vec(&delete_marker)
        .map_err(|_| AppError::InternalServerError("BSON error".into()))?;

    let index_paths =
        path_manager.get_all_metadata_paths(&uid, &fragment, &object_key, &version_id);

    let bson_arc = Arc::new(bson_data);

    let write_futures = index_paths.into_iter().map(|path| {
        let data = bson_arc.clone();
        async move {
            if let Some(parent) = path.parent() {
                if !fs::try_exists(parent).await.unwrap_or(false) {
                    fs::create_dir_all(parent)
                        .await
                        .map_err(|e| AppError::Io(e, "create index shard".into()))?;
                }
            }
            fs::write(&path, &*data)
                .await
                .map_err(|e| AppError::Io(e, "write marker".into()))
        }
    });

    let results = join_all(write_futures).await;

    let mut success_count = 0;
    for res in results {
        if res.is_ok() {
            success_count += 1;
        }
    }

    if success_count == 0 {
        return Err(AppError::InternalServerError(
            "Failed to write delete marker to any disk".into(),
        ));
    }

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

    let user_index_dirs = path_manager.get_all_user_index_dirs(&uid);
    let mut source_index_root = None;
    for dir in &user_index_dirs {
        if dir.exists() {
            source_index_root = Some(dir.clone());
            break;
        }
    }

    let fragment_name_clone = fragment.clone();

    let refund_amount = tokio::task::spawn_blocking(move || -> i64 {
        if let Some(root) = source_index_root {
            let fragment_path = root.join(&fragment_name_clone);
            let index_dir = fragment_path.join("index");

            if !index_dir.exists() {
                return 0;
            }

            let mut total_size: u64 = 0;
            for entry in WalkDir::new(&index_dir).into_iter().filter_map(|e| e.ok()) {
                if entry.file_type().is_file()
                    && entry.file_name().to_string_lossy().ends_with(".bson")
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
            return total_size as i64;
        }
        0
    })
    .await
    .unwrap_or(0);

    let blob_roots = Arc::new(path_manager.get_blob_roots());
    let uid_owned = uid.clone();
    let frag_owned = fragment.clone();
    let index_dirs_owned = user_index_dirs;

    tokio::task::spawn_blocking(move || -> Result<(), AppError> {
        for user_root in index_dirs_owned {
            let fragment_path = user_root.join(&frag_owned);
            if fragment_path.exists() {
                if let Err(e) = std::fs::remove_dir_all(&fragment_path) {
                    warn!("Failed to delete fragment index {:?}: {}", fragment_path, e);
                }
            }
        }

        for disk in blob_roots.iter() {
            let blob_frag_dir = disk
                .join(OBJECT_DATA_ROOT)
                .join(&uid_owned)
                .join(&frag_owned);
            if blob_frag_dir.exists() {
                if let Err(e) = std::fs::remove_dir_all(&blob_frag_dir) {
                    warn!(
                        "Failed to delete blob fragment dir {:?}: {}",
                        blob_frag_dir, e
                    );
                }
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
