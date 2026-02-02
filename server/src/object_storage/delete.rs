use std::sync::Arc;

use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::METADATA_EXT;
use crate::object_storage::disk_manager::{ObjectMetadata, ObjectPathManager};
use crate::object_storage::quota_manager::update_quota;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::ws_structs::StorageEventPayload;
use axum::extract::State;
use axum::{extract::Path, http::StatusCode, response::IntoResponse};
use futures_util::future::join_all;
use futures_util::{StreamExt, stream};
use sha2::{Digest, Sha256};
use std::future::Future;
use std::pin::Pin;
use tokio::fs;
use tracing::{error, info, warn};
use walkdir::WalkDir;

fn get_trash_root(disk_root: &std::path::Path, fragment_hash: &str) -> std::path::PathBuf {
    disk_root.join(fragment_hash).join(".trash")
}

pub async fn delete_object_version(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((fragment, version_id, object_key)): Path<(String, String, String)>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();
    let blob_roots = path_manager.get_blob_roots();

    let index_paths = path_manager.get_all_metadata_paths(&fragment, &object_key, &version_id);
    let mut moved_count = 0;

    for src_path in index_paths {
        if !fs::try_exists(&src_path).await.unwrap_or(false) {
            continue;
        }

        let file_name = src_path.file_name().unwrap();
        let s4_dir = src_path.parent().unwrap();
        let s4 = s4_dir.file_name().unwrap();
        let s3_dir = s4_dir.parent().unwrap();
        let s3 = s3_dir.file_name().unwrap();
        let s2_dir = s3_dir.parent().unwrap();
        let s2 = s2_dir.file_name().unwrap();
        let s1_dir = s2_dir.parent().unwrap();
        let s1 = s1_dir.file_name().unwrap();

        for root in &index_roots {
            if src_path.starts_with(root) {
                let trash_root = get_trash_root(root, &fragment);
                let dest_dir = trash_root.join("index").join(s1).join(s2).join(s3).join(s4);
                let dest_path = dest_dir.join(file_name);

                if let Err(e) = fs::create_dir_all(&dest_dir).await {
                    warn!("Failed to create trash index dir {:?}: {}", dest_dir, e);
                    continue;
                }

                if let Err(e) = fs::rename(&src_path, &dest_path).await {
                    warn!(
                        "Failed to move metadata to trash {:?} -> {:?}: {}",
                        src_path, dest_path, e
                    );
                } else {
                    moved_count += 1;
                }
                break;
            }
        }
    }

    if moved_count == 0 {
        return Err(AppError::NotFound);
    }

    for root in blob_roots {
        let src_blob_dir = path_manager.get_shard_dir_on_disk(&root, &fragment, &version_id);

        if fs::try_exists(&src_blob_dir).await.unwrap_or(false) {
            let trash_root = get_trash_root(&root, &fragment);
            let dest_blob_dir = trash_root.join("blob").join(&version_id);

            if let Some(parent) = dest_blob_dir.parent() {
                let _ = fs::create_dir_all(parent).await;
            }

            if let Err(e) = fs::rename(&src_blob_dir, &dest_blob_dir).await {
                warn!(
                    "Failed to move blob to trash {:?} -> {:?}: {}",
                    src_blob_dir, dest_blob_dir, e
                );
            }
        }
    }

    let session_dirs = path_manager.get_all_session_dirs(&fragment, &version_id);
    for src_session in session_dirs {
        if fs::try_exists(&src_session).await.unwrap_or(false) {
            for root in &index_roots {
                if src_session.starts_with(root) {
                    let trash_root = get_trash_root(root, &fragment);
                    let dest_session = trash_root.join("index").join("sessions").join(&version_id);
                    if let Some(parent) = dest_session.parent() {
                        let _ = fs::create_dir_all(parent).await;
                    }
                    let _ = fs::rename(&src_session, &dest_session).await;
                    break;
                }
            }
        }
    }

    info!(
        "Soft deleted (moved to trash) version: {}/{} (v: {})",
        fragment, object_key, version_id
    );

    state.emit(
        &fragment,
        StorageEventPayload::FileDeleted {
            fragment: fragment.clone(),
            object_key,
            version_id: Some(version_id),
        },
    );

    Ok(StatusCode::OK)
}

pub async fn delete_object(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let path_manager = ObjectPathManager::new();

    let mut hasher = Sha256::new();
    hasher.update(object_key.as_bytes());
    let key_hash = hex::encode(hasher.finalize());
    let shard1 = &key_hash[0..2];
    let shard2 = &key_hash[2..4];
    let shard3 = &key_hash[4..6];
    let shard4 = &key_hash[6..8];

    let index_dirs = path_manager.get_all_index_dirs(&fragment);

    let mut versions_found = Vec::new();

    for index_dir in index_dirs {
        let target_dir = index_dir
            .join(shard1)
            .join(shard2)
            .join(shard3)
            .join(shard4);

        if !fs::try_exists(&target_dir).await.unwrap_or(false) {
            continue;
        }

        let prefix = format!("{}#", key_hash);
        let mut entries = match fs::read_dir(&target_dir).await {
            Ok(e) => e,
            Err(_) => continue,
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with(&prefix) && name.ends_with(METADATA_EXT) {
                if let Some(rest) = name.strip_prefix(&prefix) {
                    if let Some(ver) = rest.strip_suffix(METADATA_EXT) {
                        versions_found.push(ver.to_string());
                    }
                }
            }
        }
    }

    if versions_found.is_empty() {
        return Err(AppError::NotFound);
    }

    versions_found.sort();
    versions_found.dedup();

    let mut success_count = 0;

    let index_roots = path_manager.get_all_index_roots();
    let blob_roots = path_manager.get_blob_roots();

    for version_id in versions_found {
        let index_paths = path_manager.get_all_metadata_paths(&fragment, &object_key, &version_id);
        for src_path in index_paths {
            if fs::try_exists(&src_path).await.unwrap_or(false) {
                let file_name = src_path.file_name().unwrap();
                let s4 = src_path.parent().unwrap().file_name().unwrap();
                let s3 = src_path
                    .parent()
                    .unwrap()
                    .parent()
                    .unwrap()
                    .file_name()
                    .unwrap();
                let s2 = src_path
                    .parent()
                    .unwrap()
                    .parent()
                    .unwrap()
                    .parent()
                    .unwrap()
                    .file_name()
                    .unwrap();
                let s1 = src_path
                    .parent()
                    .unwrap()
                    .parent()
                    .unwrap()
                    .parent()
                    .unwrap()
                    .parent()
                    .unwrap()
                    .file_name()
                    .unwrap();

                for root in &index_roots {
                    if src_path.starts_with(root) {
                        let trash_root = get_trash_root(root, &fragment);
                        let dest_dir = trash_root.join("index").join(s1).join(s2).join(s3).join(s4);
                        let dest_path = dest_dir.join(file_name);

                        let _ = fs::create_dir_all(&dest_dir).await;
                        let _ = fs::rename(&src_path, &dest_path).await;
                        break;
                    }
                }
            }
        }

        for root in &blob_roots {
            let src_blob = path_manager.get_shard_dir_on_disk(root, &fragment, &version_id);
            if fs::try_exists(&src_blob).await.unwrap_or(false) {
                let trash_root = get_trash_root(root, &fragment);
                let dest_blob = trash_root.join("blob").join(&version_id);
                if let Some(p) = dest_blob.parent() {
                    let _ = fs::create_dir_all(p).await;
                }
                let _ = fs::rename(&src_blob, &dest_blob).await;
            }
        }

        success_count += 1;
    }

    info!(
        "Soft deleted {} versions for object {}/{}",
        success_count, fragment, object_key
    );

    state.emit(
        &fragment,
        StorageEventPayload::FileDeleted {
            fragment: fragment.clone(),
            object_key,
            version_id: None,
        },
    );

    Ok(StatusCode::OK)
}

pub async fn empty_trash(
    State(_state): State<Arc<AppState>>,
    claims: Claims,
    Path(fragment): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.delete {
        return Err(AppError::Forbidden("Delete permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();
    let blob_roots = path_manager.get_blob_roots();

    let mut total_refund_size: u64 = 0;

    let mut calculated = false;
    for root in &index_roots {
        if calculated {
            break;
        }
        let trash_index_root = get_trash_root(root, &fragment).join("index");
        if trash_index_root.exists() {
            let mut mirror_total = 0u64;
            for entry in WalkDir::new(&trash_index_root)
                .into_iter()
                .filter_map(|e| e.ok())
            {
                if entry.file_type().is_file()
                    && entry.file_name().to_string_lossy().ends_with(METADATA_EXT)
                {
                    if let Ok(data) = std::fs::read(entry.path()) {
                        if let Ok(meta) = rmp_serde::from_slice::<ObjectMetadata>(&data) {
                            mirror_total += meta.total_size_bytes;
                        }
                    }
                }
            }
            total_refund_size = mirror_total;
            calculated = true;
        }
    }

    let mut delete_tasks: Vec<Pin<Box<dyn Future<Output = ()> + Send>>> = Vec::new();

    for root in &index_roots {
        let trash_root = get_trash_root(root, &fragment);
        delete_tasks.push(Box::pin(async move {
            if fs::try_exists(&trash_root).await.unwrap_or(false) {
                if let Err(e) = fs::remove_dir_all(&trash_root).await {
                    warn!("Failed to empty trash index root {:?}: {}", trash_root, e);
                }
            }
        }));
    }

    for root in &blob_roots {
        let trash_root = get_trash_root(root, &fragment);
        delete_tasks.push(Box::pin(async move {
            if fs::try_exists(&trash_root).await.unwrap_or(false) {
                if let Err(e) = fs::remove_dir_all(&trash_root).await {
                    warn!("Failed to empty trash blob root {:?}: {}", trash_root, e);
                }
            }
        }));
    }

    join_all(delete_tasks).await;

    if total_refund_size > 0 {
        if let Err(e) = update_quota(&fragment, -(total_refund_size as i64)).await {
            error!(
                "Failed to refund quota after emptying trash for {}: {}",
                fragment, e
            );
        } else {
            info!(
                "Refunded {} bytes after emptying trash for {}",
                total_refund_size, fragment
            );
        }
    }

    Ok(StatusCode::NO_CONTENT)
}

pub async fn delete_object_permanently(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.delete {
        return Err(AppError::Forbidden("Delete permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let path_manager = ObjectPathManager::new();

    let mut hasher = Sha256::new();
    hasher.update(object_key.as_bytes());
    let key_hash = hex::encode(hasher.finalize());
    let shard1 = &key_hash[0..2];
    let shard2 = &key_hash[2..4];
    let shard3 = &key_hash[4..6];
    let shard4 = &key_hash[6..8];

    let index_roots = path_manager.get_all_index_roots();
    let mut versions_to_delete = Vec::new();

    for root in &index_roots {
        let live_dir = root
            .join(&fragment)
            .join("index")
            .join(shard1)
            .join(shard2)
            .join(shard3)
            .join(shard4);
        let trash_dir = get_trash_root(root, &fragment)
            .join("index")
            .join(shard1)
            .join(shard2)
            .join(shard3)
            .join(shard4);

        let dirs_to_check = vec![live_dir, trash_dir];
        let prefix = format!("{}#", key_hash);
        let mut found_any = false;

        for d in dirs_to_check {
            if let Ok(mut entries) = fs::read_dir(&d).await {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if name.starts_with(&prefix) && name.ends_with(METADATA_EXT) {
                        if let Ok(data) = fs::read(entry.path()).await {
                            if let Ok(meta) = rmp_serde::from_slice::<ObjectMetadata>(&data) {
                                versions_to_delete.push((meta.version_id, meta.total_size_bytes));
                                found_any = true;
                            }
                        }
                    }
                }
            }
        }
        if found_any {
            break;
        }
    }

    if versions_to_delete.is_empty() {
        return Err(AppError::NotFound);
    }

    versions_to_delete.sort_by(|a, b| a.0.cmp(&b.0));
    versions_to_delete.dedup();

    let blob_roots = Arc::new(path_manager.get_blob_roots());
    let index_roots = Arc::new(index_roots);
    let path_manager = Arc::new(path_manager);

    let results: Vec<u64> = stream::iter(versions_to_delete)
        .map(|(version_id, size)| {
            let f = fragment.clone();
            let k = object_key.clone();
            let pm = path_manager.clone();
            let blobs = blob_roots.clone();
            let idxs = index_roots.clone();

            async move {
                let meta_paths = pm.get_all_metadata_paths(&f, &k, &version_id);
                for mp in meta_paths {
                    let _ = fs::remove_file(&mp).await;
                    for root in idxs.iter() {
                        if mp.starts_with(root) {
                            let rel = mp
                                .strip_prefix(root.join(&f))
                                .unwrap_or(&std::path::PathBuf::new())
                                .to_path_buf();
                            let trash_path = get_trash_root(root, &f).join(rel);
                            let _ = fs::remove_file(trash_path).await;
                        }
                    }
                }

                for disk in blobs.iter() {
                    let live_shard = pm.get_shard_dir_on_disk(disk, &f, &version_id);
                    let _ = fs::remove_dir_all(&live_shard).await;

                    let trash_shard = get_trash_root(disk, &f).join("blob").join(&version_id);
                    let _ = fs::remove_dir_all(&trash_shard).await;
                }

                size
            }
        })
        .buffer_unordered(20)
        .collect()
        .await;

    let total_freed: u64 = results.iter().sum();

    if total_freed > 0 {
        if let Err(e) = update_quota(&fragment, -(total_freed as i64)).await {
            error!(
                "Failed to refund quota for permanent delete {}: {}",
                fragment, e
            );
        }
    }

    info!(
        "Permanently deleted {} bytes for {}/{}",
        total_freed, fragment, object_key
    );

    state.emit(
        &fragment,
        StorageEventPayload::FileDeleted {
            fragment: fragment.clone(),
            object_key,
            version_id: None,
        },
    );

    Ok(StatusCode::NO_CONTENT)
}

pub async fn recover_object_version(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((fragment, version_id, object_key)): Path<(String, String, String)>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden(
            "Write permission required to recover".into(),
        ));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();
    let blob_roots = path_manager.get_blob_roots();

    let mut hasher = Sha256::new();
    hasher.update(object_key.as_bytes());
    let key_hash = hex::encode(hasher.finalize());
    let s1 = &key_hash[0..2];
    let s2 = &key_hash[2..4];
    let s3 = &key_hash[4..6];
    let s4 = &key_hash[6..8];
    let filename = format!("{}#{}{}", key_hash, version_id, METADATA_EXT);

    let mut recovered_count = 0;

    for root in &index_roots {
        let trash_root = get_trash_root(root, &fragment);
        let trash_file = trash_root
            .join("index")
            .join(s1)
            .join(s2)
            .join(s3)
            .join(s4)
            .join(&filename);

        if fs::try_exists(&trash_file).await.unwrap_or(false) {
            let live_dir = root
                .join(&fragment)
                .join("index")
                .join(s1)
                .join(s2)
                .join(s3)
                .join(s4);
            let live_file = live_dir.join(&filename);

            if let Err(e) = fs::create_dir_all(&live_dir).await {
                warn!("Failed to create live index dir {:?}: {}", live_dir, e);
                continue;
            }

            if let Err(e) = fs::rename(&trash_file, &live_file).await {
                warn!(
                    "Failed to recover metadata {:?} -> {:?}: {}",
                    trash_file, live_file, e
                );
            } else {
                recovered_count += 1;
            }
        }
    }

    if recovered_count == 0 {
        return Err(AppError::NotFound);
    }

    for root in &blob_roots {
        let trash_blob_dir = get_trash_root(root, &fragment)
            .join("blob")
            .join(&version_id);

        if fs::try_exists(&trash_blob_dir).await.unwrap_or(false) {
            let live_blob_dir = path_manager.get_shard_dir_on_disk(root, &fragment, &version_id);

            if let Some(parent) = live_blob_dir.parent() {
                let _ = fs::create_dir_all(parent).await;
            }

            if let Err(e) = fs::rename(&trash_blob_dir, &live_blob_dir).await {
                warn!(
                    "Failed to recover blob {:?} -> {:?}: {}",
                    trash_blob_dir, live_blob_dir, e
                );
            }
        }
    }

    info!(
        "Recovered object version: {}/{} (v: {})",
        fragment, object_key, version_id
    );

    let meta_paths = path_manager.get_all_metadata_paths(&fragment, &object_key, &version_id);
    for p in meta_paths {
        if let Ok(data) = fs::read(&p).await {
            if let Ok(meta) = rmp_serde::from_slice::<ObjectMetadata>(&data) {
                state.emit(&fragment, StorageEventPayload::FileUploaded(meta));
                break;
            }
        }
    }

    Ok(StatusCode::OK)
}

pub async fn recover_object(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden(
            "Write permission required to recover".into(),
        ));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let path_manager = ObjectPathManager::new();

    let mut hasher = Sha256::new();
    hasher.update(object_key.as_bytes());
    let key_hash = hex::encode(hasher.finalize());
    let s1 = &key_hash[0..2];
    let s2 = &key_hash[2..4];
    let s3 = &key_hash[4..6];
    let s4 = &key_hash[6..8];

    let index_roots = path_manager.get_all_index_roots();
    let blob_roots = path_manager.get_blob_roots();

    let mut found_versions = Vec::new();
    for root in &index_roots {
        let trash_dir = get_trash_root(root, &fragment)
            .join("index")
            .join(s1)
            .join(s2)
            .join(s3)
            .join(s4);
        if let Ok(mut entries) = fs::read_dir(&trash_dir).await {
            let prefix = format!("{}#", key_hash);
            while let Ok(Some(entry)) = entries.next_entry().await {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with(&prefix) && name.ends_with(METADATA_EXT) {
                    if let Some(rest) = name.strip_prefix(&prefix) {
                        if let Some(ver) = rest.strip_suffix(METADATA_EXT) {
                            found_versions.push(ver.to_string());
                        }
                    }
                }
            }
        }
    }

    if found_versions.is_empty() {
        return Err(AppError::NotFound);
    }

    found_versions.sort();
    found_versions.dedup();

    let mut recovered_count = 0;

    for version_id in found_versions {
        let filename = format!("{}#{}{}", key_hash, version_id, METADATA_EXT);

        for root in &index_roots {
            let trash_root = get_trash_root(root, &fragment);
            let trash_file = trash_root
                .join("index")
                .join(s1)
                .join(s2)
                .join(s3)
                .join(s4)
                .join(&filename);

            if fs::try_exists(&trash_file).await.unwrap_or(false) {
                let live_dir = root
                    .join(&fragment)
                    .join("index")
                    .join(s1)
                    .join(s2)
                    .join(s3)
                    .join(s4);
                let live_file = live_dir.join(&filename);

                if fs::create_dir_all(&live_dir).await.is_ok() {
                    let _ = fs::rename(&trash_file, &live_file).await;
                }
            }
        }

        for root in &blob_roots {
            let trash_blob = get_trash_root(root, &fragment)
                .join("blob")
                .join(&version_id);
            if fs::try_exists(&trash_blob).await.unwrap_or(false) {
                let live_blob = path_manager.get_shard_dir_on_disk(root, &fragment, &version_id);
                if let Some(p) = live_blob.parent() {
                    let _ = fs::create_dir_all(p).await;
                }
                let _ = fs::rename(&trash_blob, &live_blob).await;
            }
        }
        recovered_count += 1;
    }

    info!(
        "Recovered {} versions for object {}/{}",
        recovered_count, fragment, object_key
    );

    state.emit(&fragment, StorageEventPayload::StateInvalidated);

    Ok(StatusCode::OK)
}

pub async fn recover_all_trash(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path(fragment): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden(
            "Write permission required to recover all".into(),
        ));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();
    let blob_roots = path_manager.get_blob_roots();

    let mut tasks: Vec<Pin<Box<dyn Future<Output = ()> + Send>>> = Vec::new();

    for root in &index_roots {
        let trash_index_root = get_trash_root(root, &fragment).join("index");
        let live_index_root = root.join(&fragment).join("index");

        if trash_index_root.exists() {
            tasks.push(Box::pin(async move {
                for entry in WalkDir::new(&trash_index_root)
                    .into_iter()
                    .filter_map(|e| e.ok())
                {
                    if entry.file_type().is_file() {
                        let path = entry.path();
                        if let Ok(rel) = path.strip_prefix(&trash_index_root) {
                            let dest = live_index_root.join(rel);
                            if let Some(p) = dest.parent() {
                                let _ = fs::create_dir_all(p).await;
                            }
                            let _ = fs::rename(path, dest).await;
                        }
                    }
                }
            }));
        }
    }

    for root in &blob_roots {
        let trash_blob_root = get_trash_root(root, &fragment).join("blob");
        let live_blob_root = root.join(&fragment).join("blob");

        if trash_blob_root.exists() {
            tasks.push(Box::pin(async move {
                if let Ok(mut entries) = fs::read_dir(&trash_blob_root).await {
                    while let Ok(Some(entry)) = entries.next_entry().await {
                        let path = entry.path();
                        if path.is_dir() {
                            let dirname = entry.file_name();
                            let dest = live_blob_root.join(dirname);
                            if let Some(p) = dest.parent() {
                                let _ = fs::create_dir_all(p).await;
                            }
                            let _ = fs::rename(path, dest).await;
                        }
                    }
                }
            }));
        }
    }

    join_all(tasks).await;

    info!("Recovered all items from trash for fragment {}", fragment);
    state.emit(&fragment, StorageEventPayload::StateInvalidated);

    Ok(StatusCode::OK)
}
