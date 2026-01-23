use std::collections::HashSet;
use std::sync::Arc;

use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::OBJECT_DATA_ROOT;
use crate::object_storage::directory_index::DirectoryIndexManager;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{
    ObjectMetadata, ObjectPathManager, RenameObjectRequest,
};
use crate::websocket::ws_structs::StorageEventPayload;

use axum::extract::State;
use axum::{Json as AxumJson, http::StatusCode, response::IntoResponse};

use futures_util::future::join_all;
use sha2::{Digest, Sha256};
use tokio::fs;
use tracing::{error, info, warn};

const TRASH_DIR: &str = ".trash";

fn normalize_key(key: &str) -> String {
    key.trim_start_matches('/').to_string()
}

fn is_trash_root(key: &str) -> bool {
    let k = key.trim_start_matches('/').trim_end_matches('/');
    k == TRASH_DIR
}

fn is_trash_key(key: &str) -> bool {
    let k = key.trim_start_matches('/');
    if k == TRASH_DIR {
        return true;
    }
    if !k.starts_with(TRASH_DIR) {
        return false;
    }
    k.as_bytes().get(TRASH_DIR.len()) == Some(&b'/')
}

fn validate_rename_keys(
    source_key: &str,
    destination_key: &str,
    allow_trash: bool,
) -> Result<(), AppError> {
    if allow_trash {
        if is_trash_root(source_key) || is_trash_root(destination_key) {
            return Err(AppError::BadRequest(
                "Invalid trash key. Use '.trash/<path>' (not '.trash').".into(),
            ));
        }
        Ok(())
    } else {
        if is_trash_key(source_key) || is_trash_key(destination_key) {
            return Err(AppError::BadRequest(
                "'.trash' is reserved and cannot be used directly.".into(),
            ));
        }
        Ok(())
    }
}

pub async fn rename_object(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumJson(mut payload): AxumJson<RenameObjectRequest>,
) -> Result<impl IntoResponse, AppError> {
    payload.source_key = normalize_key(&payload.source_key);
    payload.destination_key = normalize_key(&payload.destination_key);

    validate_rename_keys(&payload.source_key, &payload.destination_key, false)?;

    rename_object_core(state, claims.uid, payload, false).await?;
    Ok(StatusCode::OK)
}

pub async fn rename_object_core(
    state: Arc<AppState>,
    uid: String,
    mut payload: RenameObjectRequest,
    allow_trash: bool,
) -> Result<(), AppError> {
    payload.source_key = normalize_key(&payload.source_key);
    payload.destination_key = normalize_key(&payload.destination_key);

    validate_rename_keys(&payload.source_key, &payload.destination_key, allow_trash)?;

    let path_manager = ObjectPathManager::new();

    let mut hasher = Sha256::new();
    hasher.update(payload.source_key.as_bytes());
    let src_hash = hex::encode(hasher.finalize());

    let mut hasher = Sha256::new();
    hasher.update(payload.destination_key.as_bytes());
    let dst_hash = hex::encode(hasher.finalize());

    let user_dirs = path_manager.get_all_user_index_dirs(&uid);
    let payload_arc = Arc::new(payload);

    let src_hash_arc = Arc::new(src_hash);
    let dst_hash_arc = Arc::new(dst_hash);

    let rename_futures = user_dirs.into_iter().map(|user_dir| {
        let payload = payload_arc.clone();
        let src_hash = src_hash_arc.clone();
        let dst_hash = dst_hash_arc.clone();

        async move {
            let src_shard = user_dir
                .join(&payload.source_fragment)
                .join("index")
                .join(&src_hash[0..2])
                .join(&src_hash[2..4]);

            let dst_shard = user_dir
                .join(&payload.destination_fragment)
                .join("index")
                .join(&dst_hash[0..2])
                .join(&dst_hash[2..4]);

            if !fs::try_exists(&src_shard).await.unwrap_or(false) {
                return Ok(Vec::new());
            }

            if fs::try_exists(&dst_shard).await.unwrap_or(false) {
                let prefix = format!("{}#", dst_hash);
                let mut entries = fs::read_dir(&dst_shard)
                    .await
                    .map_err(|e| AppError::Io(e, "read dest shard".into()))?;

                while let Ok(Some(entry)) = entries.next_entry().await {
                    if entry.file_name().to_string_lossy().starts_with(&prefix) {
                        return Err(AppError::Conflict(
                            "Destination object already exists".into(),
                        ));
                    }
                }
            }

            fs::create_dir_all(&dst_shard).await.map_err(|e| {
                error!("Failed to create dest shard: {}", e);
                AppError::InternalServerError("Failed to prepare rename.".into())
            })?;

            let src_prefix = format!("{}#", src_hash);

            let mut entries = fs::read_dir(&src_shard)
                .await
                .map_err(|e| AppError::Io(e, "read src shard".into()))?;

            let mut moved_versions = Vec::new();

            while let Ok(Some(entry)) = entries.next_entry().await {
                let name = entry.file_name().to_string_lossy().to_string();
                if !name.starts_with(&src_prefix) || !name.ends_with(".bson") {
                    continue;
                }

                let (_, version_ext) = match name.split_once('#') {
                    Some(v) => v,
                    None => continue,
                };
                let version_id = match version_ext.strip_suffix(".bson") {
                    Some(v) => v,
                    None => continue,
                };

                let new_filename = format!("{}#{}.bson", dst_hash, version_id);
                let dest_path = dst_shard.join(&new_filename);

                let data = match fs::read(entry.path()).await {
                    Ok(d) => d,
                    Err(e) => {
                        warn!("Failed to read src meta during rename: {}", e);
                        continue;
                    }
                };

                let mut meta = match mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                    Ok(m) => m,
                    Err(e) => {
                        warn!("Corrupt meta during rename (skipping): {}", e);
                        continue;
                    }
                };

                meta.object_key = payload.destination_key.clone();
                meta.fragment = payload.destination_fragment.clone();

                let bytes = match mongodb::bson::to_vec(&meta) {
                    Ok(b) => b,
                    Err(e) => {
                        warn!("Failed to serialize rewritten meta: {}", e);
                        continue;
                    }
                };

                if let Err(e) = fs::write(&dest_path, bytes).await {
                    warn!("Failed to write dest meta during rename: {}", e);
                    continue;
                }

                let _ = fs::remove_file(entry.path()).await;
                moved_versions.push(version_id.to_string());
            }

            Ok(moved_versions)
        }
    });

    let results = join_all(rename_futures).await;

    let mut affected_versions = HashSet::new();
    let mut success = false;

    for result in results {
        match result {
            Ok(versions) => {
                if !versions.is_empty() {
                    success = true;
                    for v in versions {
                        affected_versions.insert(v);
                    }
                }
            }
            Err(e) => return Err(e),
        }
    }

    if !success {
        return Err(AppError::NotFound);
    }

    if payload_arc.source_fragment != payload_arc.destination_fragment {
        let blob_roots = path_manager.get_blob_roots();
        let index_roots = path_manager.get_all_user_index_dirs(&uid);

        let all_disks_to_migrate: Vec<std::path::PathBuf> = blob_roots
            .iter()
            .map(|r| r.join(OBJECT_DATA_ROOT).join(&uid))
            .chain(index_roots.into_iter())
            .collect();

        for user_root in all_disks_to_migrate {
            for version_id in &affected_versions {
                let src_blob_dir = user_root
                    .join(&payload_arc.source_fragment)
                    .join("blobs")
                    .join(version_id);

                let dst_blob_dir = user_root
                    .join(&payload_arc.destination_fragment)
                    .join("blobs")
                    .join(version_id);

                if src_blob_dir.exists() {
                    if let Some(parent) = dst_blob_dir.parent() {
                        if !parent.exists() {
                            let _ = fs::create_dir_all(parent).await;
                        }
                    }

                    if let Err(e) = fs::rename(&src_blob_dir, &dst_blob_dir).await {
                        warn!(
                            "Failed to rename blob dir {:?} -> {:?}: {}",
                            src_blob_dir, dst_blob_dir, e
                        );
                    }
                }
            }
        }
    }

    info!(
        "Renamed object from {}/{} to {}/{}",
        payload_arc.source_fragment,
        payload_arc.source_key,
        payload_arc.destination_fragment,
        payload_arc.destination_key
    );

    DirectoryIndexManager::trigger_background_removal(
        uid.clone(),
        payload_arc.source_fragment.clone(),
        payload_arc.source_key.clone(),
    );

    DirectoryIndexManager::trigger_background_insert(
        uid.clone(),
        payload_arc.destination_fragment.clone(),
        payload_arc.destination_key.clone(),
    );

    state.emit(
        &uid,
        StorageEventPayload::FileRenamed {
            source_fragment: payload_arc.source_fragment.clone(),
            source_key: payload_arc.source_key.clone(),
            destination_fragment: payload_arc.destination_fragment.clone(),
            destination_key: payload_arc.destination_key.clone(),
        },
    );

    Ok(())
}
