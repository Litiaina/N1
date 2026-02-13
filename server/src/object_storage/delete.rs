use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::extract::{Path as AxumPath, State};
use axum::{http::StatusCode, response::IntoResponse};

use futures_util::{stream, StreamExt};
use sha2::{Digest, Sha256};
use tokio::fs;
use tracing::{error, info, warn};
use ulid::Ulid;
use walkdir::WalkDir;

use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::{METADATA_EXT, QUOTA_FILE};
use crate::object_storage::disk_manager::{
    default_stripe_size, ObjectMetadata, ObjectPathManager, PrefixIndexEntry, PrefixIndexSegment,
};
use crate::object_storage::prefix_compactor::schedule_prefix_compaction;
use crate::object_storage::quota_manager::update_quota;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::ws_structs::StorageEventPayload;

fn get_trash_root(disk_root: &std::path::Path, fragment_hash: &str) -> std::path::PathBuf {
    disk_root.join(fragment_hash).join(".trash")
}

fn is_cross_device_rename(err: &std::io::Error) -> bool {
    #[cfg(unix)]
    {
        err.raw_os_error() == Some(libc::EXDEV)
    }
    #[cfg(not(unix))]
    {
        let _ = err;
        false
    }
}

async fn move_file_or_copy(src: &std::path::Path, dst: &std::path::Path) -> Result<(), AppError> {
    if let Some(parent) = dst.parent() {
        let _ = fs::create_dir_all(parent).await;
    }

    match fs::rename(src, dst).await {
        Ok(_) => Ok(()),
        Err(e) => {
            if is_cross_device_rename(&e) {
                fs::copy(src, dst).await.map_err(|e| {
                    AppError::Io(e, format!("copy {} -> {}", src.display(), dst.display()))
                })?;
                fs::remove_file(src)
                    .await
                    .map_err(|e| AppError::Io(e, format!("remove {}", src.display())))?;
                Ok(())
            } else {
                Err(AppError::Io(
                    e,
                    format!("rename {} -> {}", src.display(), dst.display()),
                ))
            }
        }
    }
}

async fn move_dir_or_copy(src: &std::path::Path, dst: &std::path::Path) -> Result<(), AppError> {
    if let Some(parent) = dst.parent() {
        let _ = fs::create_dir_all(parent).await;
    }

    match fs::rename(src, dst).await {
        Ok(_) => Ok(()),
        Err(e) => {
            if !is_cross_device_rename(&e) {
                return Err(AppError::Io(
                    e,
                    format!("rename dir {} -> {}", src.display(), dst.display()),
                ));
            }

            let src = src.to_path_buf();
            let dst = dst.to_path_buf();

            tokio::task::spawn_blocking(move || -> Result<(), AppError> {
                use std::fs as stdfs;

                stdfs::create_dir_all(&dst).map_err(|e| {
                    AppError::Io(e, format!("mkdir {} (copydir fallback)", dst.display()))
                })?;

                for entry in WalkDir::new(&src).into_iter().filter_map(|e| e.ok()) {
                    let p = entry.path();
                    let rel = match p.strip_prefix(&src) {
                        Ok(r) => r,
                        Err(_) => continue,
                    };
                    if rel.as_os_str().is_empty() {
                        continue;
                    }
                    let target = dst.join(rel);

                    if entry.file_type().is_dir() {
                        let _ = stdfs::create_dir_all(&target);
                        continue;
                    }

                    if entry.file_type().is_file() {
                        if let Some(parent) = target.parent() {
                            let _ = stdfs::create_dir_all(parent);
                        }
                        stdfs::copy(p, &target).map_err(|e| {
                            AppError::Io(e, format!("copy {} -> {}", p.display(), target.display()))
                        })?;
                    }
                }

                stdfs::remove_dir_all(&src).map_err(|e| {
                    AppError::Io(
                        e,
                        format!("remove_dir_all {} (after copydir)", src.display()),
                    )
                })?;

                Ok(())
            })
            .await
            .map_err(|e| AppError::InternalServerError(format!("copydir task: {}", e)))??;

            Ok(())
        }
    }
}

#[cfg(unix)]
pub async fn sync_dir(path: &std::path::Path) -> Result<(), AppError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        use std::fs::OpenOptions;
        use std::os::unix::fs::OpenOptionsExt;

        let f = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY)
            .open(&path)
            .map_err(|e| AppError::Io(e, format!("open dir for sync: {:?}", path)))?;

        f.sync_all()
            .map_err(|e| AppError::Io(e, format!("sync dir: {:?}", path)))?;

        Ok::<(), AppError>(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("sync dir task: {}", e)))?
}

#[cfg(windows)]
pub async fn sync_dir(path: &std::path::Path) -> Result<(), AppError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        use std::fs::OpenOptions;
        use std::os::windows::fs::OpenOptionsExt;

        const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x02000000;

        let result = OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(&path);

        match result {
            Ok(file) => {
                let _ = file.sync_all();
            }
            Err(e) => {
                warn!(
                    "Windows: Failed to open directory for sync (non-fatal): {} - {}",
                    path.display(),
                    e
                );
            }
        }

        Ok::<(), AppError>(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("sync dir task: {}", e)))?
}

#[cfg(not(any(unix, windows)))]
pub async fn sync_dir(_path: &std::path::Path) -> Result<(), AppError> {
    Ok(())
}

fn canonical_prefix(prefix: &str) -> String {
    prefix.trim_start_matches('/').to_string()
}

fn prefixes_for_key(object_key: &str) -> BTreeSet<String> {
    let key = canonical_prefix(object_key);
    let mut out = BTreeSet::new();
    out.insert(String::new());

    if key.is_empty() {
        return out;
    }

    let parts: Vec<&str> = key.split('/').filter(|p| !p.is_empty()).collect();
    let mut cur = String::new();

    for (i, p) in parts.iter().enumerate() {
        if i == 0 {
            cur.push_str(p);
        } else {
            cur.push('/');
            cur.push_str(p);
        }
        out.insert(cur.clone());
    }

    out
}

async fn write_atomic(path: &std::path::Path, data: &[u8]) -> Result<(), AppError> {
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent).await;
    }

    let tmp = path.with_file_name(format!(
        ".tmp_{}_{}",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("file"),
        Ulid::new().to_string()
    ));

    fs::write(&tmp, data)
        .await
        .map_err(|e| AppError::Io(e, format!("write tmp {}", tmp.display())))?;

    fs::rename(&tmp, path)
        .await
        .map_err(|e| AppError::Io(e, format!("rename tmp -> {}", path.display())))?;

    if let Some(parent) = path.parent() {
        let _ = sync_dir(parent).await;
    }

    Ok(())
}

async fn write_atomic_if_missing(path: &std::path::Path, data: &[u8]) -> Result<bool, AppError> {
    if fs::try_exists(path).await.unwrap_or(false) {
        return Ok(false);
    }
    write_atomic(path, data).await?;
    Ok(true)
}

async fn read_metadata_any(
    path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Option<ObjectMetadata> {
    let paths = path_manager.get_all_metadata_paths(fragment, object_key, version_id);
    for p in paths {
        if !fs::try_exists(&p).await.unwrap_or(false) {
            continue;
        }
        if let Ok(data) = fs::read(&p).await {
            if let Ok(meta) = rmp_serde::from_slice::<ObjectMetadata>(&data) {
                return Some(meta);
            }
        }
    }
    None
}

async fn write_prefix_segments_for_meta(
    path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
    version_id: &str,
    created_at: i64,
) -> Result<(), AppError> {
    let prefixes = prefixes_for_key(object_key);
    let mut any_ok = false;

    for prefix in prefixes {
        let seg = PrefixIndexSegment {
            prefix: prefix.clone(),
            entries: vec![PrefixIndexEntry {
                object_key: object_key.to_string(),
                version_id: version_id.to_string(),
                created_at,
            }],
        };

        let bytes = rmp_serde::to_vec(&seg).map_err(|e| {
            AppError::InternalServerError(format!("Serialize prefix segment: {}", e))
        })?;

        let paths = path_manager.get_all_prefix_segment_paths(fragment, &prefix, version_id);

        let mut wrote_this_prefix = false;
        for p in paths {
            if let Some(parent) = p.parent() {
                let _ = fs::create_dir_all(parent).await;
            }
            if write_atomic_if_missing(&p, &bytes).await.unwrap_or(false) {
                wrote_this_prefix = true;
            }
        }

        if wrote_this_prefix {
            any_ok = true;
        }
    }

    if !any_ok {
        return Err(AppError::InternalServerError(
            "Failed to write prefix index segment to any mirror".into(),
        ));
    }

    Ok(())
}

async fn invalidate_compacted_for_key(
    path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
) {
    let prefixes = prefixes_for_key(object_key);
    let mut compact_candidates = Vec::new();
    for pfx in prefixes {
        let cpfx = canonical_prefix(&pfx);
        compact_candidates.push(cpfx.clone());
        for p in path_manager.get_all_prefix_compacted_paths(fragment, &cpfx) {
            let _ = fs::remove_file(&p).await;
            if let Some(parent) = p.parent() {
                let _ = sync_dir(parent).await;
            }
        }
    }
    schedule_prefix_compaction(fragment.to_string(), compact_candidates);
}

async fn write_delete_marker_for_key(
    path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
    template: Option<&ObjectMetadata>,
) -> Result<(String, i64), AppError> {
    let now = chrono::Utc::now().timestamp();
    let delete_version_id = Ulid::new().to_string();

    let mut dm = template.cloned().unwrap_or_else(|| ObjectMetadata {
        object_key: object_key.to_string(),
        fragment: fragment.to_string(),
        version_id: delete_version_id.clone(),
        upload_session_id: String::new(),
        owner_uid: fragment.to_string(),
        content_type: "application/octet-stream".to_string(),
        total_size_bytes: 0,
        created_at: now,
        parts: Vec::new(),
        is_delete_marker: true,
        stripe_size_bytes: default_stripe_size(),
    });

    dm.fragment = fragment.to_string();
    dm.owner_uid = fragment.to_string();
    dm.object_key = object_key.to_string();
    dm.version_id = delete_version_id.clone();
    dm.created_at = now;
    dm.is_delete_marker = true;
    dm.parts.clear();
    dm.total_size_bytes = 0;

    let bytes = rmp_serde::to_vec(&dm)
        .map_err(|e| AppError::InternalServerError(format!("Serialize delete marker: {}", e)))?;

    let paths = path_manager.get_all_metadata_paths(fragment, object_key, &delete_version_id);
    let mut wrote_any = false;
    for p in paths {
        if let Some(parent) = p.parent() {
            let _ = fs::create_dir_all(parent).await;
        }
        if write_atomic(&p, &bytes).await.is_ok() {
            wrote_any = true;
        }
    }

    if !wrote_any {
        return Err(AppError::InternalServerError(
            "Failed to write delete marker metadata to any index root".into(),
        ));
    }

    Ok((delete_version_id, now))
}

async fn purge_delete_markers_for_key(
    path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
) {
    let versions = match list_object_versions_any(path_manager, fragment, object_key).await {
        Ok(v) => v,
        Err(_) => return,
    };

    for version_id in versions {
        let Some(meta) = read_metadata_any(path_manager, fragment, object_key, &version_id).await
        else {
            continue;
        };

        if !meta.is_delete_marker {
            continue;
        }

        for p in path_manager.get_all_metadata_paths(fragment, object_key, &version_id) {
            let _ = fs::remove_file(&p).await;
            if let Some(parent) = p.parent() {
                let _ = sync_dir(parent).await;
            }
        }
    }
}

async fn list_object_versions_any(
    path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
) -> Result<Vec<String>, AppError> {
    let mut hasher = Sha256::new();
    hasher.update(object_key.as_bytes());
    let key_hash = hex::encode(hasher.finalize());

    let shard1 = &key_hash[0..2];
    let shard2 = &key_hash[2..4];
    let shard3 = &key_hash[4..6];
    let shard4 = &key_hash[6..8];

    let index_roots = path_manager.get_all_index_roots();

    let mut versions_found = Vec::new();
    for root in index_roots {
        let target_dir = root
            .join(fragment)
            .join("index")
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

    versions_found.sort();
    versions_found.dedup();
    Ok(versions_found)
}

pub async fn delete_object_version(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumPath((fragment, version_id, object_key)): AxumPath<(String, String, String)>,
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

    let mut hasher = Sha256::new();
    hasher.update(object_key.as_bytes());
    let key_hash = hex::encode(hasher.finalize());
    let shard1 = &key_hash[0..2];
    let shard2 = &key_hash[2..4];
    let shard3 = &key_hash[4..6];
    let shard4 = &key_hash[6..8];
    let filename = format!("{}#{}{}", key_hash, version_id, METADATA_EXT);
    let template_meta = read_metadata_any(&path_manager, &fragment, &object_key, &version_id).await;

    let mut moved_count = 0usize;

    for root in &index_roots {
        let src_path = root
            .join(&fragment)
            .join("index")
            .join(shard1)
            .join(shard2)
            .join(shard3)
            .join(shard4)
            .join(&filename);

        if !fs::try_exists(&src_path).await.unwrap_or(false) {
            continue;
        }

        let trash_root = get_trash_root(root, &fragment);
        let dest_path = trash_root
            .join("index")
            .join(shard1)
            .join(shard2)
            .join(shard3)
            .join(shard4)
            .join(&filename);

        match move_file_or_copy(&src_path, &dest_path).await {
            Ok(_) => moved_count += 1,
            Err(e) => warn!("Failed to move meta to trash: {}", e),
        }
    }

    if moved_count == 0 {
        return Err(AppError::NotFound);
    }

    for root in &blob_roots {
        let src_blob_dir = path_manager.get_shard_dir_on_disk(root, &fragment, &version_id);
        if !fs::try_exists(&src_blob_dir).await.unwrap_or(false) {
            continue;
        }

        let trash_root = get_trash_root(root, &fragment);
        let dest_blob_dir = trash_root.join("blob").join(&version_id);

        match move_dir_or_copy(&src_blob_dir, &dest_blob_dir).await {
            Ok(_) => {}
            Err(e) => warn!("Failed to move blob to trash: {}", e),
        }
    }

    for root in &index_roots {
        let src_session = root
            .join(&fragment)
            .join("index")
            .join("sessions")
            .join(&version_id);

        if !fs::try_exists(&src_session).await.unwrap_or(false) {
            continue;
        }

        let trash_root = get_trash_root(root, &fragment);
        let dest_session = trash_root.join("index").join("sessions").join(&version_id);

        match move_dir_or_copy(&src_session, &dest_session).await {
            Ok(_) => {}
            Err(e) => warn!("Failed to move session to trash: {}", e),
        }
    }

    let remaining_versions = list_object_versions_any(&path_manager, &fragment, &object_key)
        .await
        .unwrap_or_default();
    let mut has_remaining_non_marker = false;
    for v in &remaining_versions {
        if let Some(m) = read_metadata_any(&path_manager, &fragment, &object_key, v).await {
            if !m.is_delete_marker {
                has_remaining_non_marker = true;
                break;
            }
        }
    }

    if !has_remaining_non_marker {
        if let Ok((del_vid, del_created_at)) = write_delete_marker_for_key(
            &path_manager,
            &fragment,
            &object_key,
            template_meta.as_ref(),
        )
        .await
        {
            let _ = write_prefix_segments_for_meta(
                &path_manager,
                &fragment,
                &object_key,
                &del_vid,
                del_created_at,
            )
            .await;
        }
    }

    invalidate_compacted_for_key(&path_manager, &fragment, &object_key).await;

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
    AxumPath((fragment, object_key)): AxumPath<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let path_manager = ObjectPathManager::new();
    let versions_found = list_object_versions_any(&path_manager, &fragment, &object_key).await?;

    if versions_found.is_empty() {
        return Err(AppError::NotFound);
    }

    let mut template_meta: Option<ObjectMetadata> = None;
    for v in &versions_found {
        if let Some(m) = read_metadata_any(&path_manager, &fragment, &object_key, v).await {
            template_meta = Some(m);
            break;
        }
    }

    let index_roots = path_manager.get_all_index_roots();
    let blob_roots = path_manager.get_blob_roots();

    let mut hasher = Sha256::new();
    hasher.update(object_key.as_bytes());
    let key_hash = hex::encode(hasher.finalize());
    let shard1 = key_hash[0..2].to_string();
    let shard2 = key_hash[2..4].to_string();
    let shard3 = key_hash[4..6].to_string();
    let shard4 = key_hash[6..8].to_string();

    let mut success_count = 0usize;

    for version_id in &versions_found {
        let filename = format!("{}#{}{}", key_hash, version_id, METADATA_EXT);

        for root in &index_roots {
            let src = root
                .join(&fragment)
                .join("index")
                .join(&shard1)
                .join(&shard2)
                .join(&shard3)
                .join(&shard4)
                .join(&filename);

            if fs::try_exists(&src).await.unwrap_or(false) {
                let trash_root = get_trash_root(root, &fragment);
                let dst = trash_root
                    .join("index")
                    .join(&shard1)
                    .join(&shard2)
                    .join(&shard3)
                    .join(&shard4)
                    .join(&filename);

                let _ = move_file_or_copy(&src, &dst).await;
            }

            let src_session = root
                .join(&fragment)
                .join("index")
                .join("sessions")
                .join(version_id);

            if fs::try_exists(&src_session).await.unwrap_or(false) {
                let trash_root = get_trash_root(root, &fragment);
                let dst_session = trash_root.join("index").join("sessions").join(version_id);
                let _ = move_dir_or_copy(&src_session, &dst_session).await;
            }
        }

        for root in &blob_roots {
            let src_blob = path_manager.get_shard_dir_on_disk(root, &fragment, version_id);
            if fs::try_exists(&src_blob).await.unwrap_or(false) {
                let trash_root = get_trash_root(root, &fragment);
                let dst_blob = trash_root.join("blob").join(version_id);
                let _ = move_dir_or_copy(&src_blob, &dst_blob).await;
            }
        }

        success_count += 1;
    }

    if let Ok((del_vid, del_created_at)) = write_delete_marker_for_key(
        &path_manager,
        &fragment,
        &object_key,
        template_meta.as_ref(),
    )
    .await
    {
        let _ = write_prefix_segments_for_meta(
            &path_manager,
            &fragment,
            &object_key,
            &del_vid,
            del_created_at,
        )
        .await;
    }

    invalidate_compacted_for_key(&path_manager, &fragment, &object_key).await;

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
    AxumPath(fragment): AxumPath<String>,
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

    futures_util::future::join_all(delete_tasks).await;

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
    AxumPath((fragment, object_key)): AxumPath<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.delete {
        return Err(AppError::Forbidden("Delete permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();

    let key_hash = ObjectPathManager::object_key_hash(&object_key);
    let shard1 = key_hash[0..2].to_string();
    let shard2 = key_hash[2..4].to_string();
    let shard3 = key_hash[4..6].to_string();
    let shard4 = key_hash[6..8].to_string();
    let prefix = format!("{}#", key_hash);

    let mut versions_map: HashMap<String, u64> = HashMap::new();

    for root in &index_roots {
        let live_dir = root
            .join(&fragment)
            .join("index")
            .join(&shard1)
            .join(&shard2)
            .join(&shard3)
            .join(&shard4);

        let trash_dir = get_trash_root(root, &fragment)
            .join("index")
            .join(&shard1)
            .join(&shard2)
            .join(&shard3)
            .join(&shard4);

        let mut found_any = false;

        for dir in [&live_dir, &trash_dir] {
            if let Ok(mut entries) = fs::read_dir(dir).await {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if !name.starts_with(&prefix) || !name.ends_with(METADATA_EXT) {
                        continue;
                    }

                    let version_id = match name.strip_prefix(&prefix) {
                        Some(rest) => match rest.strip_suffix(METADATA_EXT) {
                            Some(ver) => ver.to_string(),
                            None => continue,
                        },
                        None => continue,
                    };

                    let mut size = 0u64;
                    if let Ok(data) = fs::read(entry.path()).await {
                        if let Ok(meta) = rmp_serde::from_slice::<ObjectMetadata>(&data) {
                            size = meta.total_size_bytes;
                        }
                    }

                    versions_map.entry(version_id).or_insert(size);
                    found_any = true;
                }
            }
        }

        if found_any {
            break;
        }
    }

    if versions_map.is_empty() {
        return Err(AppError::NotFound);
    }

    let mut versions_to_delete: Vec<(String, u64)> = versions_map.into_iter().collect();
    versions_to_delete.sort_by(|a, b| a.0.cmp(&b.0));
    versions_to_delete.dedup_by(|a, b| a.0 == b.0);

    let blob_roots = Arc::new(path_manager.get_blob_roots());
    let index_roots = Arc::new(index_roots);
    let path_manager = Arc::new(path_manager);

    let key_hash_arc = Arc::new(key_hash);
    let shard1_arc = Arc::new(shard1);
    let shard2_arc = Arc::new(shard2);
    let shard3_arc = Arc::new(shard3);
    let shard4_arc = Arc::new(shard4);

    let results: Vec<u64> = stream::iter(versions_to_delete)
        .map(|(version_id, size)| {
            let f = fragment.clone();
            let pm = path_manager.clone();
            let blobs = blob_roots.clone();
            let idxs = index_roots.clone();

            let key_hash = key_hash_arc.clone();
            let s1 = shard1_arc.clone();
            let s2 = shard2_arc.clone();
            let s3 = shard3_arc.clone();
            let s4 = shard4_arc.clone();

            async move {
                let filename = format!("{}#{}{}", *key_hash, version_id, METADATA_EXT);

                for root in idxs.iter() {
                    let live_path = root
                        .join(&f)
                        .join("index")
                        .join(&*s1)
                        .join(&*s2)
                        .join(&*s3)
                        .join(&*s4)
                        .join(&filename);

                    let trash_path = get_trash_root(root, &f)
                        .join("index")
                        .join(&*s1)
                        .join(&*s2)
                        .join(&*s3)
                        .join(&*s4)
                        .join(&filename);

                    let _ = fs::remove_file(&live_path).await;
                    let _ = fs::remove_file(&trash_path).await;

                    let live_session = root
                        .join(&f)
                        .join("index")
                        .join("sessions")
                        .join(&version_id);

                    let trash_session = get_trash_root(root, &f)
                        .join("index")
                        .join("sessions")
                        .join(&version_id);

                    let _ = fs::remove_dir_all(&live_session).await;
                    let _ = fs::remove_dir_all(&trash_session).await;
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

    let pm = ObjectPathManager::new();

    invalidate_compacted_for_key(&pm, &fragment, &object_key).await;

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
    AxumPath((fragment, version_id, object_key)): AxumPath<(String, String, String)>,
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

    let key_hash = ObjectPathManager::object_key_hash(&object_key);
    let s1 = &key_hash[0..2];
    let s2 = &key_hash[2..4];
    let s3 = &key_hash[4..6];
    let s4 = &key_hash[6..8];
    let filename = format!("{}#{}{}", key_hash, version_id, METADATA_EXT);

    let mut recovered_count = 0usize;

    for root in &index_roots {
        let trash_file = get_trash_root(root, &fragment)
            .join("index")
            .join(s1)
            .join(s2)
            .join(s3)
            .join(s4)
            .join(&filename);

        if !fs::try_exists(&trash_file).await.unwrap_or(false) {
            continue;
        }

        let live_dir = root
            .join(&fragment)
            .join("index")
            .join(s1)
            .join(s2)
            .join(s3)
            .join(s4);

        let live_file = live_dir.join(&filename);

        let _ = fs::create_dir_all(&live_dir).await;

        match move_file_or_copy(&trash_file, &live_file).await {
            Ok(_) => recovered_count += 1,
            Err(e) => warn!("Failed to recover metadata: {}", e),
        }

        let trash_session = get_trash_root(root, &fragment)
            .join("index")
            .join("sessions")
            .join(&version_id);

        if fs::try_exists(&trash_session).await.unwrap_or(false) {
            let live_session = root
                .join(&fragment)
                .join("index")
                .join("sessions")
                .join(&version_id);

            let _ = fs::create_dir_all(
                live_session
                    .parent()
                    .unwrap_or_else(|| std::path::Path::new("/")),
            )
            .await;

            let _ = move_dir_or_copy(&trash_session, &live_session).await;
        }
    }

    if recovered_count == 0 {
        return Err(AppError::NotFound);
    }

    purge_delete_markers_for_key(&path_manager, &fragment, &object_key).await;

    for root in &blob_roots {
        let trash_blob_dir = get_trash_root(root, &fragment)
            .join("blob")
            .join(&version_id);

        if !fs::try_exists(&trash_blob_dir).await.unwrap_or(false) {
            continue;
        }

        let live_blob_dir = path_manager.get_shard_dir_on_disk(root, &fragment, &version_id);
        if let Some(parent) = live_blob_dir.parent() {
            let _ = fs::create_dir_all(parent).await;
        }

        let _ = move_dir_or_copy(&trash_blob_dir, &live_blob_dir).await;
    }

    if let Some(meta) = read_metadata_any(&path_manager, &fragment, &object_key, &version_id).await
    {
        let _ = write_prefix_segments_for_meta(
            &path_manager,
            &fragment,
            &meta.object_key,
            &meta.version_id,
            meta.created_at,
        )
        .await;
    }

    invalidate_compacted_for_key(&path_manager, &fragment, &object_key).await;

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
    AxumPath((fragment, object_key)): AxumPath<(String, String)>,
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

    let key_hash = ObjectPathManager::object_key_hash(&object_key);
    let s1 = &key_hash[0..2];
    let s2 = &key_hash[2..4];
    let s3 = &key_hash[4..6];
    let s4 = &key_hash[6..8];

    let index_roots = path_manager.get_all_index_roots();
    let blob_roots = path_manager.get_blob_roots();

    let mut found_versions = Vec::new();
    let prefix = format!("{}#", key_hash);

    for root in &index_roots {
        let trash_dir = get_trash_root(root, &fragment)
            .join("index")
            .join(s1)
            .join(s2)
            .join(s3)
            .join(s4);

        if let Ok(mut entries) = fs::read_dir(&trash_dir).await {
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
    purge_delete_markers_for_key(&path_manager, &fragment, &object_key).await;

    let mut recovered_count = 0usize;

    for version_id in &found_versions {
        let filename = format!("{}#{}{}", key_hash, version_id, METADATA_EXT);

        for root in &index_roots {
            let trash_file = get_trash_root(root, &fragment)
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
                let _ = fs::create_dir_all(&live_dir).await;
                let _ = move_file_or_copy(&trash_file, &live_file).await;
            }

            let trash_session = get_trash_root(root, &fragment)
                .join("index")
                .join("sessions")
                .join(version_id);

            if fs::try_exists(&trash_session).await.unwrap_or(false) {
                let live_session = root
                    .join(&fragment)
                    .join("index")
                    .join("sessions")
                    .join(version_id);

                let _ = fs::create_dir_all(
                    live_session
                        .parent()
                        .unwrap_or_else(|| std::path::Path::new("/")),
                )
                .await;

                let _ = move_dir_or_copy(&trash_session, &live_session).await;
            }
        }

        for root in &blob_roots {
            let trash_blob = get_trash_root(root, &fragment)
                .join("blob")
                .join(version_id);

            if fs::try_exists(&trash_blob).await.unwrap_or(false) {
                let live_blob = path_manager.get_shard_dir_on_disk(root, &fragment, version_id);
                if let Some(p) = live_blob.parent() {
                    let _ = fs::create_dir_all(p).await;
                }
                let _ = move_dir_or_copy(&trash_blob, &live_blob).await;
            }
        }

        if let Some(meta) =
            read_metadata_any(&path_manager, &fragment, &object_key, version_id).await
        {
            let _ = write_prefix_segments_for_meta(
                &path_manager,
                &fragment,
                &meta.object_key,
                &meta.version_id,
                meta.created_at,
            )
            .await;
        }

        recovered_count += 1;
    }

    invalidate_compacted_for_key(&path_manager, &fragment, &object_key).await;

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
    AxumPath(fragment): AxumPath<String>,
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

    let mut recovered_meta_entries: HashMap<String, (String, String, i64)> = HashMap::new();

    for root in &index_roots {
        let trash_index_root = get_trash_root(root, &fragment).join("index");
        if !trash_index_root.exists() {
            continue;
        }

        for entry in WalkDir::new(&trash_index_root)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if !entry.file_type().is_file() {
                continue;
            }

            let name = entry.file_name().to_string_lossy().to_string();
            if !name.ends_with(METADATA_EXT) || name == QUOTA_FILE {
                continue;
            }

            let data = match std::fs::read(entry.path()) {
                Ok(d) => d,
                Err(_) => continue,
            };

            let meta = match rmp_serde::from_slice::<ObjectMetadata>(&data) {
                Ok(m) => m,
                Err(_) => continue,
            };

            let k = format!("{}\n{}", meta.object_key, meta.version_id);
            recovered_meta_entries.insert(k, (meta.object_key, meta.version_id, meta.created_at));
        }
    }

    let mut tasks: Vec<Pin<Box<dyn Future<Output = ()> + Send>>> = Vec::new();

    for root in index_roots {
        let trash_index_root = get_trash_root(&root, &fragment).join("index");
        let live_index_root = root.join(&fragment).join("index");

        if trash_index_root.exists() {
            tasks.push(Box::pin(async move {
                for entry in WalkDir::new(&trash_index_root)
                    .into_iter()
                    .filter_map(|e| e.ok())
                {
                    if entry.file_type().is_file() {
                        let path = entry.path().to_path_buf();
                        let rel = match path.strip_prefix(&trash_index_root) {
                            Ok(r) => r.to_path_buf(),
                            Err(_) => continue,
                        };
                        let dest = live_index_root.join(rel);
                        let _ = move_file_or_copy(&path, &dest).await;
                    }
                }

                let _ = fs::remove_dir_all(&trash_index_root).await;
            }));
        }
    }

    for root in blob_roots {
        let trash_blob_root = get_trash_root(&root, &fragment).join("blob");
        let live_blob_root = root.join(&fragment).join("blob");

        if trash_blob_root.exists() {
            tasks.push(Box::pin(async move {
                if let Ok(mut entries) = fs::read_dir(&trash_blob_root).await {
                    while let Ok(Some(entry)) = entries.next_entry().await {
                        let path = entry.path();
                        if path.is_dir() {
                            let dirname = entry.file_name();
                            let dest = live_blob_root.join(dirname);
                            let _ = move_dir_or_copy(&path, &dest).await;
                        }
                    }
                }

                let _ = fs::remove_dir_all(&trash_blob_root).await;
            }));
        }
    }

    futures_util::future::join_all(tasks).await;

    let mut affected_prefixes: BTreeSet<String> = BTreeSet::new();
    for (_k, (object_key, version_id, created_at)) in recovered_meta_entries.iter() {
        let _ = write_prefix_segments_for_meta(
            &path_manager,
            &fragment,
            object_key,
            version_id,
            *created_at,
        )
        .await;
        purge_delete_markers_for_key(&path_manager, &fragment, object_key).await;

        for pfx in prefixes_for_key(object_key) {
            affected_prefixes.insert(pfx);
        }
    }

    for pfx in affected_prefixes {
        invalidate_compacted_for_key(&path_manager, &fragment, &pfx).await;
    }

    info!("Recovered all items from trash for fragment {}", fragment);
    state.emit(&fragment, StorageEventPayload::StateInvalidated);

    Ok(StatusCode::OK)
}
