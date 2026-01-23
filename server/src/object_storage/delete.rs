use std::sync::Arc;

use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::OBJECT_DATA_ROOT;
use crate::object_storage::directory_index::DirectoryIndexManager;
use crate::object_storage::quota_manager::update_quota;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{ObjectMetadata, ObjectPathManager};
use crate::websocket::ws_structs::StorageEventPayload;

use axum::extract::State;
use axum::{Json, extract::Path, http::StatusCode, response::IntoResponse};

use futures_util::future::join_all;
use futures_util::{StreamExt, stream};
use sha2::{Digest, Sha256};
use tokio::fs;
use tracing::{error, info, warn};
use walkdir::WalkDir;

const TRASH_DIR: &str = ".trash";

fn normalize_key(key: &str) -> String {
    key.trim_start_matches('/').to_string()
}

fn is_trash_key(key: &str) -> bool {
    let k = key.trim_start_matches('/');
    k == TRASH_DIR || k.starts_with(&format!("{}/", TRASH_DIR))
}

fn validate_reserved_paths(object_key: &str) -> Result<(), AppError> {
    if is_trash_key(object_key) {
        return Err(AppError::BadRequest(
            "'.trash' is reserved and cannot be used directly.".into(),
        ));
    }
    Ok(())
}

fn hash_key(object_key: &str) -> (String, String, String) {
    let mut hasher = Sha256::new();
    hasher.update(object_key.as_bytes());
    let hash = hex::encode(hasher.finalize());
    let shard1 = hash[0..2].to_string();
    let shard2 = hash[2..4].to_string();
    (hash, shard1, shard2)
}

fn meta_filename(hash: &str, version_id: &str) -> String {
    format!("{}#{}.bson", hash, version_id)
}

fn get_trash_meta_paths(
    path_manager: &ObjectPathManager,
    uid: &str,
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Vec<std::path::PathBuf> {
    let (hash, s1, s2) = hash_key(object_key);
    let filename = meta_filename(&hash, version_id);

    path_manager
        .get_all_user_index_dirs(uid)
        .iter()
        .map(|user_dir| {
            user_dir
                .join(fragment)
                .join(TRASH_DIR)
                .join("index")
                .join(&s1)
                .join(&s2)
                .join(&filename)
        })
        .collect()
}

fn get_trash_blob_dir(
    disk_root: &std::path::PathBuf,
    uid: &str,
    fragment: &str,
    version_id: &str,
) -> std::path::PathBuf {
    disk_root
        .join(OBJECT_DATA_ROOT)
        .join(uid)
        .join(fragment)
        .join(TRASH_DIR)
        .join("blobs")
        .join(version_id)
}

async fn list_versions_in_shard_dir(shard_dir: &std::path::PathBuf, hash: &str) -> Vec<String> {
    let mut out = Vec::new();
    let prefix = format!("{}#", hash);

    let mut entries = match fs::read_dir(shard_dir).await {
        Ok(e) => e,
        Err(_) => return out,
    };

    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.starts_with(&prefix) || !name.ends_with(".bson") {
            continue;
        }
        let ver_part = match name.strip_prefix(&prefix) {
            Some(v) => v,
            None => continue,
        };
        let ver = match ver_part.strip_suffix(".bson") {
            Some(v) => v,
            None => continue,
        };
        out.push(ver.to_string());
    }

    out
}

async fn any_active_versions_remain(
    path_manager: &ObjectPathManager,
    uid: &str,
    fragment: &str,
    object_key: &str,
) -> bool {
    let (hash, s1, s2) = hash_key(object_key);
    let prefix = format!("{}#", hash);

    for dir in path_manager.get_all_index_dirs(uid, fragment) {
        let shard_dir = dir.join(&s1).join(&s2);
        if !fs::try_exists(&shard_dir).await.unwrap_or(false) {
            continue;
        }
        if let Ok(mut rd) = fs::read_dir(&shard_dir).await {
            while let Ok(Some(entry)) = rd.next_entry().await {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with(&prefix) && name.ends_with(".bson") {
                    return true;
                }
            }
        }
    }
    false
}

async fn move_meta_version_to_trash(
    path_manager: &ObjectPathManager,
    uid: &str,
    fragment: &str,
    object_key: &str,
    version_id: &str,
) {
    let active_paths = path_manager.get_all_metadata_paths(uid, fragment, object_key, version_id);
    let trash_paths = get_trash_meta_paths(path_manager, uid, fragment, object_key, version_id);

    for (src, dst) in active_paths.into_iter().zip(trash_paths.into_iter()) {
        if !fs::try_exists(&src).await.unwrap_or(false) {
            continue;
        }
        if let Some(parent) = dst.parent() {
            let _ = fs::create_dir_all(parent).await;
        }
        if let Err(e) = fs::rename(&src, &dst).await {
            warn!("Failed to move meta to trash {:?} -> {:?}: {}", src, dst, e);
        }
    }
}

async fn move_blobs_version_to_trash(
    blob_roots: &[std::path::PathBuf],
    uid: &str,
    fragment: &str,
    version_id: &str,
) {
    let path_manager = ObjectPathManager::new();

    for disk in blob_roots {
        let active_dir = path_manager.get_shard_dir_on_disk(disk, uid, fragment, version_id);
        if !fs::try_exists(&active_dir).await.unwrap_or(false) {
            continue;
        }

        let trash_dir = get_trash_blob_dir(disk, uid, fragment, version_id);
        if let Some(parent) = trash_dir.parent() {
            let _ = fs::create_dir_all(parent).await;
        }

        if let Err(e) = fs::rename(&active_dir, &trash_dir).await {
            warn!(
                "Failed to move blob dir to trash {:?} -> {:?}: {}",
                active_dir, trash_dir, e
            );
        }
    }
}

pub async fn delete_object(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((fragment, object_key_raw)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let object_key = normalize_key(&object_key_raw);
    validate_reserved_paths(&object_key)?;

    let path_manager = ObjectPathManager::new();
    let (hash, s1, s2) = hash_key(&object_key);

    let mut active_shard_dir: Option<std::path::PathBuf> = None;
    for dir in path_manager.get_all_index_dirs(&uid, &fragment) {
        let candidate = dir.join(&s1).join(&s2);
        if fs::try_exists(&candidate).await.unwrap_or(false) {
            active_shard_dir = Some(candidate);
            break;
        }
    }

    let Some(shard_dir) = active_shard_dir else {
        DirectoryIndexManager::trigger_background_removal(
            uid.clone(),
            fragment.clone(),
            object_key.clone(),
        );
        state.emit(
            &uid,
            StorageEventPayload::FileDeleted {
                fragment,
                object_key,
                version_id: None,
            },
        );
        return Ok(StatusCode::NO_CONTENT);
    };

    let versions = list_versions_in_shard_dir(&shard_dir, &hash).await;
    if versions.is_empty() {
        DirectoryIndexManager::trigger_background_removal(
            uid.clone(),
            fragment.clone(),
            object_key.clone(),
        );
        state.emit(
            &uid,
            StorageEventPayload::FileDeleted {
                fragment,
                object_key,
                version_id: None,
            },
        );
        return Ok(StatusCode::NO_CONTENT);
    }

    let blob_roots = path_manager.get_blob_roots();

    for v in &versions {
        move_meta_version_to_trash(&path_manager, &uid, &fragment, &object_key, v).await;
        move_blobs_version_to_trash(&blob_roots, &uid, &fragment, v).await;
    }

    DirectoryIndexManager::trigger_background_removal(
        uid.clone(),
        fragment.clone(),
        object_key.clone(),
    );

    info!(
        "Soft deleted (trashed): {}/{} (versions: {})",
        fragment,
        object_key,
        versions.len()
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

pub async fn delete_object_version(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((fragment, version_id, object_key_raw)): Path<(String, String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let object_key = normalize_key(&object_key_raw);
    validate_reserved_paths(&object_key)?;

    let path_manager = ObjectPathManager::new();

    let mut size_to_free: i64 = 0;

    let active_meta_paths =
        path_manager.get_all_metadata_paths(&uid, &fragment, &object_key, &version_id);
    for p in &active_meta_paths {
        if let Ok(data) = fs::read(p).await {
            if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                if !meta.is_delete_marker {
                    size_to_free = meta.total_size_bytes as i64;
                    break;
                }
            }
        }
    }

    if size_to_free == 0 {
        let trash_meta_paths =
            get_trash_meta_paths(&path_manager, &uid, &fragment, &object_key, &version_id);
        for p in &trash_meta_paths {
            if let Ok(data) = fs::read(p).await {
                if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                    if !meta.is_delete_marker {
                        size_to_free = meta.total_size_bytes as i64;
                        break;
                    }
                }
            }
        }
    }

    let trash_meta_paths =
        get_trash_meta_paths(&path_manager, &uid, &fragment, &object_key, &version_id);

    let meta_delete_futures = active_meta_paths
        .into_iter()
        .chain(trash_meta_paths.into_iter())
        .map(|p| async move {
            let _ = fs::remove_file(&p).await;
        });

    join_all(meta_delete_futures).await;

    let session_dirs = path_manager.get_all_session_dirs(&uid, &fragment, &version_id);
    let _ = join_all(session_dirs.into_iter().map(|d| async move {
        let _ = fs::remove_dir_all(&d).await;
    }))
    .await;

    let blob_roots = path_manager.get_blob_roots();

    let _ = join_all(blob_roots.iter().map(|disk| {
        let uid = uid.clone();
        let fragment = fragment.clone();
        let version_id = version_id.clone();
        let disk = disk.clone();
        async move {
            let pm2 = ObjectPathManager::new();
            let active = pm2.get_shard_dir_on_disk(&disk, &uid, &fragment, &version_id);
            let trash = get_trash_blob_dir(&disk, &uid, &fragment, &version_id);
            let _ = fs::remove_dir_all(&active).await;
            let _ = fs::remove_dir_all(&trash).await;
        }
    }))
    .await;

    if size_to_free > 0 {
        if let Err(e) = update_quota(&uid, -size_to_free).await {
            error!("Failed to refund quota for user {}: {}", uid, e);
        }
    }

    if !any_active_versions_remain(&path_manager, &uid, &fragment, &object_key).await {
        DirectoryIndexManager::trigger_background_removal(
            uid.clone(),
            fragment.clone(),
            object_key.clone(),
        );
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
    Path((fragment, object_key_raw)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let object_key = normalize_key(&object_key_raw);
    validate_reserved_paths(&object_key)?;

    let path_manager = ObjectPathManager::new();
    let (hash, s1, s2) = hash_key(&object_key);

    let mut active_shard_dir: Option<std::path::PathBuf> = None;
    for dir in path_manager.get_all_index_dirs(&uid, &fragment) {
        let candidate = dir.join(&s1).join(&s2);
        if fs::try_exists(&candidate).await.unwrap_or(false) {
            active_shard_dir = Some(candidate);
            break;
        }
    }

    let mut trash_shard_dir: Option<std::path::PathBuf> = None;
    for user_dir in path_manager.get_all_user_index_dirs(&uid) {
        let candidate = user_dir
            .join(&fragment)
            .join(TRASH_DIR)
            .join("index")
            .join(&s1)
            .join(&s2);
        if fs::try_exists(&candidate).await.unwrap_or(false) {
            trash_shard_dir = Some(candidate);
            break;
        }
    }

    let mut versions = Vec::new();
    if let Some(d) = &active_shard_dir {
        versions.extend(list_versions_in_shard_dir(d, &hash).await);
    }
    if let Some(d) = &trash_shard_dir {
        versions.extend(list_versions_in_shard_dir(d, &hash).await);
    }
    versions.sort();
    versions.dedup();

    if versions.is_empty() {
        DirectoryIndexManager::trigger_background_removal(
            uid.clone(),
            fragment.clone(),
            object_key.clone(),
        );
        state.emit(
            &uid,
            StorageEventPayload::FileDeleted {
                fragment,
                object_key,
                version_id: None,
            },
        );
        return Ok(StatusCode::NO_CONTENT);
    }

    let blob_roots = Arc::new(path_manager.get_blob_roots());
    let path_manager = Arc::new(path_manager);

    let refunds: Vec<i64> = stream::iter(versions)
        .map(|version_id| {
            let u = uid.clone();
            let f = fragment.clone();
            let k = object_key.clone();
            let pm = path_manager.clone();
            let disks = blob_roots.clone();

            async move {
                let mut size: i64 = 0;

                let active_paths = pm.get_all_metadata_paths(&u, &f, &k, &version_id);
                for p in &active_paths {
                    if let Ok(data) = fs::read(p).await {
                        if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                            if !meta.is_delete_marker {
                                size = meta.total_size_bytes as i64;
                                break;
                            }
                        }
                    }
                }
                if size == 0 {
                    let trash_paths = get_trash_meta_paths(&pm, &u, &f, &k, &version_id);
                    for p in &trash_paths {
                        if let Ok(data) = fs::read(p).await {
                            if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                                if !meta.is_delete_marker {
                                    size = meta.total_size_bytes as i64;
                                    break;
                                }
                            }
                        }
                    }
                }

                let trash_paths = get_trash_meta_paths(&pm, &u, &f, &k, &version_id);
                for p in active_paths.into_iter().chain(trash_paths.into_iter()) {
                    let _ = fs::remove_file(&p).await;
                }

                for sd in pm.get_all_session_dirs(&u, &f, &version_id) {
                    let _ = fs::remove_dir_all(&sd).await;
                }

                for disk in disks.iter() {
                    let active_dir = pm.get_shard_dir_on_disk(disk, &u, &f, &version_id);
                    let trash_dir = get_trash_blob_dir(disk, &u, &f, &version_id);
                    let _ = fs::remove_dir_all(&active_dir).await;
                    let _ = fs::remove_dir_all(&trash_dir).await;
                }

                size
            }
        })
        .buffer_unordered(20)
        .collect()
        .await;

    let total_size_to_free: i64 = refunds.iter().sum();

    if total_size_to_free > 0 {
        if let Err(e) = update_quota(&uid, -total_size_to_free).await {
            error!("Failed to refund quota for user {}: {}", uid, e);
        }
    }

    DirectoryIndexManager::trigger_background_removal(
        uid.clone(),
        fragment.clone(),
        object_key.clone(),
    );

    info!(
        "Permanently deleted object: {}/{} (Freed: {} bytes)",
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
        let Some(root) = source_index_root else {
            return 0;
        };

        let mut total_size: u64 = 0;

        let active_index = root.join(&fragment_name_clone).join("index");
        if active_index.exists() {
            for entry in WalkDir::new(&active_index)
                .into_iter()
                .filter_map(|e| e.ok())
            {
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
        }

        let trash_index = root
            .join(&fragment_name_clone)
            .join(TRASH_DIR)
            .join("index");
        if trash_index.exists() {
            for entry in WalkDir::new(&trash_index)
                .into_iter()
                .filter_map(|e| e.ok())
            {
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
        }

        total_size as i64
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

    state.emit(
        &uid,
        StorageEventPayload::FragmentDeleted {
            fragment: fragment.clone(),
        },
    );

    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "success",
            "message": "Fragment deleted successfully"
        })),
    ))
}

fn get_active_meta_paths(
    path_manager: &ObjectPathManager,
    uid: &str,
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Vec<std::path::PathBuf> {
    let (hash, s1, s2) = hash_key(object_key);
    let filename = meta_filename(&hash, version_id);

    path_manager
        .get_all_user_index_dirs(uid)
        .iter()
        .map(|user_dir| {
            user_dir
                .join(fragment)
                .join("index")
                .join(&s1)
                .join(&s2)
                .join(&filename)
        })
        .collect()
}

fn get_active_blob_dir(
    disk_root: &std::path::PathBuf,
    uid: &str,
    fragment: &str,
    version_id: &str,
) -> std::path::PathBuf {
    disk_root
        .join(OBJECT_DATA_ROOT)
        .join(uid)
        .join(fragment)
        .join("blobs")
        .join(version_id)
}

async fn list_versions_in_trash(
    path_manager: &ObjectPathManager,
    uid: &str,
    fragment: &str,
    object_key: &str,
) -> Vec<String> {
    let (hash, s1, s2) = hash_key(object_key);
    let mut all = Vec::new();

    for user_dir in path_manager.get_all_user_index_dirs(uid) {
        let shard_dir = user_dir
            .join(fragment)
            .join(TRASH_DIR)
            .join("index")
            .join(&s1)
            .join(&s2);

        if fs::try_exists(&shard_dir).await.unwrap_or(false) {
            all.extend(list_versions_in_shard_dir(&shard_dir, &hash).await);
        }
    }

    all.sort();
    all.dedup();
    all
}

async fn move_meta_version_from_trash_to_active(
    path_manager: &ObjectPathManager,
    uid: &str,
    fragment: &str,
    object_key: &str,
    version_id: &str,
) {
    let trash_paths = get_trash_meta_paths(path_manager, uid, fragment, object_key, version_id);
    let active_paths = get_active_meta_paths(path_manager, uid, fragment, object_key, version_id);

    for (src, dst) in trash_paths.into_iter().zip(active_paths.into_iter()) {
        if !fs::try_exists(&src).await.unwrap_or(false) {
            continue;
        }
        if let Some(parent) = dst.parent() {
            let _ = fs::create_dir_all(parent).await;
        }
        let _ = fs::rename(&src, &dst).await;
    }
}

async fn move_blobs_version_from_trash_to_active(
    blob_roots: &[std::path::PathBuf],
    uid: &str,
    fragment: &str,
    version_id: &str,
) {
    for disk in blob_roots {
        let src = get_trash_blob_dir(disk, uid, fragment, version_id);
        if !fs::try_exists(&src).await.unwrap_or(false) {
            continue;
        }

        let dst = get_active_blob_dir(disk, uid, fragment, version_id);
        if let Some(parent) = dst.parent() {
            let _ = fs::create_dir_all(parent).await;
        }

        let _ = fs::rename(&src, &dst).await;
    }
}

async fn active_version_exists(
    path_manager: &ObjectPathManager,
    uid: &str,
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> bool {
    for p in get_active_meta_paths(path_manager, uid, fragment, object_key, version_id) {
        if fs::try_exists(&p).await.unwrap_or(false) {
            return true;
        }
    }
    false
}

async fn trash_version_exists(
    path_manager: &ObjectPathManager,
    uid: &str,
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> bool {
    for p in get_trash_meta_paths(path_manager, uid, fragment, object_key, version_id) {
        if fs::try_exists(&p).await.unwrap_or(false) {
            return true;
        }
    }
    false
}

pub async fn restore_object(
    State(_state): State<Arc<AppState>>,
    claims: Claims,
    Path((fragment, object_key_raw)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let object_key = normalize_key(&object_key_raw);
    validate_reserved_paths(&object_key)?;

    let path_manager = ObjectPathManager::new();

    if any_active_versions_remain(&path_manager, &uid, &fragment, &object_key).await {
        return Err(AppError::Conflict("Object already exists".into()));
    }

    let versions = list_versions_in_trash(&path_manager, &uid, &fragment, &object_key).await;
    if versions.is_empty() {
        return Err(AppError::NotFound);
    }

    let blob_roots = path_manager.get_blob_roots();

    for v in &versions {
        move_meta_version_from_trash_to_active(&path_manager, &uid, &fragment, &object_key, v)
            .await;
        move_blobs_version_from_trash_to_active(&blob_roots, &uid, &fragment, v).await;
    }

    DirectoryIndexManager::trigger_background_insert(uid, fragment, object_key);

    Ok(StatusCode::OK)
}

pub async fn restore_object_version(
    State(_state): State<Arc<AppState>>,
    claims: Claims,
    Path((fragment, version_id, object_key_raw)): Path<(String, String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let object_key = normalize_key(&object_key_raw);
    validate_reserved_paths(&object_key)?;

    let path_manager = ObjectPathManager::new();

    if active_version_exists(&path_manager, &uid, &fragment, &object_key, &version_id).await {
        return Err(AppError::Conflict("Version already exists".into()));
    }

    if !trash_version_exists(&path_manager, &uid, &fragment, &object_key, &version_id).await {
        return Err(AppError::NotFound);
    }

    let blob_roots = path_manager.get_blob_roots();

    move_meta_version_from_trash_to_active(
        &path_manager,
        &uid,
        &fragment,
        &object_key,
        &version_id,
    )
    .await;
    move_blobs_version_from_trash_to_active(&blob_roots, &uid, &fragment, &version_id).await;

    DirectoryIndexManager::trigger_background_insert(uid, fragment, object_key);

    Ok(StatusCode::OK)
}
