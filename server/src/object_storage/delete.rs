use crate::enums::app_error_enum::AppError;
use crate::object_storage::caching::invalidate_cache;
use crate::object_storage::constants::{OBJECT_DATA_ROOT, OBJECT_META_FILE};
use crate::object_storage::quota_manager::update_quota;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{ObjectMetadata, ObjectPathManager};
use axum::Json;
use axum::{extract::Path, http::StatusCode, response::IntoResponse};
use chrono::Utc;
use futures_util::{StreamExt, stream};
use tokio::fs;
use tracing::{error, info, warn};
use ulid::Ulid;
use walkdir::WalkDir;

pub async fn delete_object_version(
    claims: Claims,
    Path((fragment, version_id, object_key)): Path<(String, String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let path_manager = ObjectPathManager::new();
    let version_dir = path_manager.get_version_dir(&uid, &fragment, &object_key, &version_id);

    if !fs::try_exists(&version_dir).await.unwrap_or(false) {
        return Err(AppError::NotFound);
    }

    let meta_path = version_dir.join(OBJECT_META_FILE);
    let mut size_to_free = 0;

    if let Ok(data) = fs::read(&meta_path).await {
        if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
            if !meta.is_delete_marker {
                size_to_free = meta.total_size_bytes as i64;
            }
        }
    }

    fs::remove_dir_all(&version_dir).await.map_err(|e| {
        error!("Failed to delete version directory: {}", e);
        AppError::InternalServerError("Failed to delete version.".to_string())
    })?;

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

    Ok(StatusCode::NO_CONTENT)
}

pub async fn delete_object_permanently(
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let path_manager = ObjectPathManager::new();
    let object_dir = path_manager.get_object_dir(&uid, &fragment, &object_key);

    if !fs::try_exists(&object_dir).await.unwrap_or(false) {
        return Err(AppError::NotFound);
    }

    let mut version_paths = Vec::new();
    let mut entries = fs::read_dir(&object_dir)
        .await
        .map_err(|e| AppError::Io(e, "read object dir".into()))?;

    while let Ok(Some(entry)) = entries.next_entry().await {
        if let Ok(ft) = entry.file_type().await {
            if ft.is_dir() {
                version_paths.push(entry.path());
            }
        }
    }

    let results: Vec<i64> = stream::iter(version_paths)
        .map(|path| async move {
            let mut freed_bytes = 0;

            let mut has_subdirs = false;
            if let Ok(mut sub_entries) = fs::read_dir(&path).await {
                while let Ok(Some(sub)) = sub_entries.next_entry().await {
                    if let Ok(sub_ft) = sub.file_type().await {
                        if sub_ft.is_dir() {
                            has_subdirs = true;
                            break;
                        }
                    }
                }
            }

            if has_subdirs {
                return 0;
            }

            let meta_path = path.join(OBJECT_META_FILE);
            if let Ok(data) = fs::read(&meta_path).await {
                if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                    if !meta.is_delete_marker {
                        freed_bytes = meta.total_size_bytes as i64;
                    }
                }
            }

            if let Err(e) = fs::remove_dir_all(&path).await {
                error!("Failed to delete version dir {:?}: {}", path, e);
                return 0;
            }

            freed_bytes
        })
        .buffer_unordered(20)
        .collect()
        .await;

    let total_size_to_free: i64 = results.iter().sum();

    let _ = fs::remove_dir(&object_dir).await;

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

    Ok(StatusCode::NO_CONTENT)
}

pub async fn delete_object(
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let version_id = Ulid::new().to_string();
    let session_id = Ulid::new().to_string();

    let path_manager = ObjectPathManager::new();
    let version_dir = path_manager.get_version_dir(&uid, &fragment, &object_key, &version_id);

    fs::create_dir_all(&version_dir)
        .await
        .map_err(|e| AppError::Io(e, "create marker dir".into()))?;

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

    let meta_path = version_dir.join(OBJECT_META_FILE);
    fs::write(&meta_path, bson_data)
        .await
        .map_err(|e| AppError::Io(e, "write marker".into()))?;

    invalidate_cache(&fragment, &object_key, &uid).await;
    info!("Created delete marker: {}/{}", fragment, object_key);

    Ok(StatusCode::NO_CONTENT)
}

pub async fn delete_fragment(
    claims: Claims,
    Path(fragment): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let path_manager = ObjectPathManager::new();

    let user_storage_root = path_manager.root.join(OBJECT_DATA_ROOT).join(&uid);
    let fragment_path = user_storage_root.join(&fragment);

    let scan_path = fragment_path.clone();
    let refund_amount = tokio::task::spawn_blocking(move || -> i64 {
        if !scan_path.exists() {
            return 0;
        }

        let mut total_size: u64 = 0;
        for entry in WalkDir::new(&scan_path).into_iter().filter_map(|e| e.ok()) {
            if entry.file_name() == OBJECT_META_FILE {
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

    tokio::task::spawn_blocking(move || -> Result<(), AppError> {
        if !fragment_path.exists() {
            return Err(AppError::NotFound);
        }

        std::fs::remove_dir_all(&fragment_path).map_err(|e| {
            error!("Failed to delete fragment {:?}: {}", fragment_path, e);
            AppError::InternalServerError("Failed to delete fragment.".to_string())
        })?;

        Ok(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("Task join error: {}", e)))??;

    info!("Deleted fragment '{}' for user {}", fragment, uid);

    if refund_amount > 0 {
        if let Err(e) = update_quota(&uid, -refund_amount).await {
            warn!(
                "Failed to refund quota after fragment delete for {}: {}",
                uid, e
            );
        } else {
            info!(
                "Refunded {} bytes to user {} after fragment delete",
                refund_amount, uid
            );
        }
    }

    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "success",
            "message": "Fragment deleted successfully"
        })),
    ))
}
