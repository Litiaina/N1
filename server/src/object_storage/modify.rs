use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use axum::extract::State;
use axum::{Json as AxumJson, http::StatusCode, response::IntoResponse};
use chrono::Utc;
use futures_util::future::join_all;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::{error, info, warn};
use ulid::Ulid;

use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::{METADATA_EXT, PREFIX_SEGMENT_EXT};
use crate::object_storage::disk_manager::{
    ObjectMetadata, ObjectPathManager, PrefixIndexEntry, PrefixIndexSegment, RenameObjectRequest,
};
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::ws_structs::StorageEventPayload;

#[cfg(unix)]
pub async fn sync_dir(path: &Path) -> Result<(), AppError> {
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
pub async fn sync_dir(path: &Path) -> Result<(), AppError> {
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
pub async fn sync_dir(_path: &Path) -> Result<(), AppError> {
    Ok(())
}

pub async fn write_atomic(path: &Path, data: &[u8]) -> Result<(), AppError> {
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError(format!("No parent dir for {}", path.display()))
    })?;

    fs::create_dir_all(parent)
        .await
        .map_err(|e| AppError::Io(e, "create parent dir".into()))?;

    let tmp = parent.join(format!(
        ".{}.tmp.{}",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("file"),
        Ulid::new().to_string()
    ));

    let mut f = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .await
        .map_err(|e| AppError::Io(e, format!("open tmp {}", tmp.display())))?;

    f.write_all(data)
        .await
        .map_err(|e| AppError::Io(e, "write tmp".into()))?;

    f.sync_all()
        .await
        .map_err(|e| AppError::Io(e, "sync tmp".into()))?;
    drop(f);

    fs::rename(&tmp, path)
        .await
        .map_err(|e| AppError::Io(e, format!("rename {} -> {}", tmp.display(), path.display())))?;

    sync_dir(parent).await?;
    Ok(())
}

async fn write_atomic_if_missing(path: &Path, data: &[u8]) -> Result<bool, AppError> {
    if fs::try_exists(path).await.unwrap_or(false) {
        return Ok(false);
    }
    write_atomic(path, data).await?;
    Ok(true)
}

fn canonical_prefix(prefix: &str) -> String {
    prefix.trim_matches('/').to_string()
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
    let mut failures = Vec::new();

    for prefix in prefixes {
        let segment = PrefixIndexSegment {
            prefix: prefix.clone(),
            entries: vec![PrefixIndexEntry {
                object_key: object_key.to_string(),
                version_id: version_id.to_string(),
                created_at,
            }],
        };

        let data = rmp_serde::to_vec(&segment).map_err(|e| {
            AppError::InternalServerError(format!("Serialize prefix segment: {}", e))
        })?;

        let paths = path_manager.get_all_prefix_segment_paths(fragment, &prefix, version_id);
        let futs = paths.into_iter().map(|p| {
            let d = data.clone();
            async move {
                if let Some(parent) = p.parent() {
                    let _ = fs::create_dir_all(parent).await;
                }
                match write_atomic_if_missing(&p, &d).await {
                    Ok(_) => true,
                    Err(e) => {
                        warn!("Failed to write prefix segment {:?}: {}", p, e);
                        false
                    }
                }
            }
        });

        let results = join_all(futs).await;
        if !results.into_iter().any(|ok| ok) {
            failures.push(prefix);
        }
    }

    if !failures.is_empty() {
        return Err(AppError::InternalServerError(format!(
            "Failed to write prefix index segment for prefixes: {:?}",
            failures
        )));
    }

    Ok(())
}

async fn remove_all_prefix_segments(
    path_manager: &ObjectPathManager,
    fragment: &str,
    prefix: &str,
) {
    let pfx = canonical_prefix(prefix);

    for dir in path_manager.get_all_prefix_bucket_dirs(fragment, &pfx) {
        if !fs::try_exists(&dir).await.unwrap_or(false) {
            continue;
        }
        if let Ok(mut entries) = fs::read_dir(&dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with("seg_") && name.ends_with(PREFIX_SEGMENT_EXT) {
                    let _ = fs::remove_file(entry.path()).await;
                }
            }
        }
    }

    for p in path_manager.get_all_prefix_compacted_paths(fragment, &pfx) {
        let _ = fs::remove_file(&p).await;
    }
}

async fn rebuild_prefix_bucket_for_prefix(
    path_manager: &ObjectPathManager,
    fragment: &str,
    prefix: &str,
) -> Result<(), AppError> {
    let pfx = canonical_prefix(prefix);

    let bucket_dirs = path_manager.get_all_prefix_bucket_dirs(fragment, &pfx);
    let mut bucket_dir = None;
    for d in bucket_dirs {
        if fs::try_exists(&d).await.unwrap_or(false) {
            bucket_dir = Some(d);
            break;
        }
    }

    let mut segments: Vec<PrefixIndexSegment> = Vec::new();

    for cp in path_manager.get_all_prefix_compacted_paths(fragment, &pfx) {
        if fs::try_exists(&cp).await.unwrap_or(false) {
            if let Ok(data) = fs::read(&cp).await {
                if let Ok(seg) = rmp_serde::from_slice::<PrefixIndexSegment>(&data) {
                    segments.push(seg);
                    break;
                }
            }
        }
    }

    if let Some(bucket_dir) = bucket_dir.as_ref() {
        let mut seg_files = Vec::new();
        if let Ok(mut entries) = fs::read_dir(bucket_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                if !entry
                    .file_type()
                    .await
                    .map(|t| t.is_file())
                    .unwrap_or(false)
                {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with("seg_") && name.ends_with(PREFIX_SEGMENT_EXT) {
                    seg_files.push((name, entry.path()));
                }
            }
        }
        seg_files.sort_by(|a, b| a.0.cmp(&b.0));

        for (_name, path) in seg_files {
            if let Ok(data) = fs::read(&path).await {
                if let Ok(seg) = rmp_serde::from_slice::<PrefixIndexSegment>(&data) {
                    segments.push(seg);
                }
            }
        }
    }

    if segments.is_empty() {
        return Ok(());
    }

    let mut all_entries: Vec<(String, String, i64)> = Vec::new();
    for seg in segments {
        if canonical_prefix(&seg.prefix) != pfx {
            continue;
        }
        for e in seg.entries {
            all_entries.push((e.object_key, e.version_id, e.created_at));
        }
    }

    if all_entries.is_empty() {
        return Ok(());
    }

    all_entries.sort_by(|a, b| {
        let (_ak, av, ats) = a;
        let (_bk, bv, bts) = b;
        match bts.cmp(ats) {
            std::cmp::Ordering::Equal => bv.cmp(av),
            other => other,
        }
    });

    let mut chosen: HashMap<String, (String, i64)> = HashMap::new();

    for (k, v, _ts) in all_entries {
        if chosen.contains_key(&k) {
            continue;
        }

        if let Some(meta) = read_metadata_any(path_manager, fragment, &k, &v).await {
            if meta.is_delete_marker {
                continue;
            }
            let ck = canonical_prefix(&meta.object_key);
            if !pfx.is_empty() && ck != pfx && !ck.starts_with(&(pfx.clone() + "/")) {
                continue;
            }
            chosen.insert(k, (v, meta.created_at));
        }
    }

    let mut out_entries: Vec<PrefixIndexEntry> = chosen
        .into_iter()
        .map(|(object_key, (version_id, created_at))| PrefixIndexEntry {
            object_key,
            version_id,
            created_at,
        })
        .collect();

    out_entries.sort_by(|a, b| match a.object_key.cmp(&b.object_key) {
        std::cmp::Ordering::Equal => a.version_id.cmp(&b.version_id),
        other => other,
    });

    let compacted = PrefixIndexSegment {
        prefix: pfx.clone(),
        entries: out_entries,
    };

    let bytes = rmp_serde::to_vec(&compacted)
        .map_err(|e| AppError::InternalServerError(format!("Serialize prefix compacted: {}", e)))?;

    let mut any_ok = false;
    for p in path_manager.get_all_prefix_compacted_paths(fragment, &pfx) {
        if write_atomic(&p, &bytes).await.is_ok() {
            any_ok = true;
        }
    }

    if !any_ok {
        remove_all_prefix_segments(path_manager, fragment, &pfx).await;
        return Ok(());
    }

    for dir in path_manager.get_all_prefix_bucket_dirs(fragment, &pfx) {
        if !fs::try_exists(&dir).await.unwrap_or(false) {
            continue;
        }
        if let Ok(mut entries) = fs::read_dir(&dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with("seg_") && name.ends_with(PREFIX_SEGMENT_EXT) {
                    let _ = fs::remove_file(entry.path()).await;
                }
            }
        }
    }

    Ok(())
}

async fn rebuild_prefix_indexes_for_key(
    path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
) -> Result<(), AppError> {
    let prefixes = prefixes_for_key(object_key);
    for pfx in prefixes {
        let _ = rebuild_prefix_bucket_for_prefix(path_manager, fragment, &pfx).await;
    }
    Ok(())
}

async fn write_delete_marker_for_key(
    path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
    template: &ObjectMetadata,
) -> Result<(String, i64), AppError> {
    let now = Utc::now().timestamp();
    let delete_version_id = Ulid::new().to_string();

    let mut dm = template.clone();
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

async fn delete_source_metadata_versions(
    path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
    versions: &HashSet<String>,
) {
    for v in versions {
        for p in path_manager.get_all_metadata_paths(fragment, object_key, v) {
            let _ = fs::remove_file(&p).await;
            if let Some(parent) = p.parent() {
                let _ = sync_dir(parent).await;
            }
        }
    }
}

pub async fn rename_object(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumJson(payload): AxumJson<RenameObjectRequest>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden("Modify permission required".into()));
    }

    if claims.fragment != payload.source_fragment {
        return Err(AppError::Forbidden(
            "Access denied to source fragment".into(),
        ));
    }

    if payload.source_fragment != payload.destination_fragment {
        if claims.fragment != payload.destination_fragment {
            return Err(AppError::Forbidden(
                "Access denied to destination fragment".into(),
            ));
        }
    }

    let path_manager = ObjectPathManager::new();

    let src_key_hash = ObjectPathManager::object_key_hash(&payload.source_key);
    let dst_key_hash = ObjectPathManager::object_key_hash(&payload.destination_key);

    let index_roots = path_manager.get_all_index_roots();
    let payload_arc = Arc::new(payload);
    let src_key_hash_arc = Arc::new(src_key_hash);
    let dst_key_hash_arc = Arc::new(dst_key_hash);

    let rename_futures = index_roots.into_iter().map(|index_root| {
        let payload = payload_arc.clone();
        let src_key_hash = src_key_hash_arc.clone();
        let dst_key_hash = dst_key_hash_arc.clone();

        async move {
            let src_shard = index_root
                .join(&payload.source_fragment)
                .join("index")
                .join(&src_key_hash[0..2])
                .join(&src_key_hash[2..4])
                .join(&src_key_hash[4..6])
                .join(&src_key_hash[6..8]);

            let dst_shard = index_root
                .join(&payload.destination_fragment)
                .join("index")
                .join(&dst_key_hash[0..2])
                .join(&dst_key_hash[2..4])
                .join(&dst_key_hash[4..6])
                .join(&dst_key_hash[6..8]);

            if !fs::try_exists(&src_shard).await.unwrap_or(false) {
                return Ok::<Vec<(String, i64, ObjectMetadata)>, AppError>(Vec::new());
            }

            if fs::try_exists(&dst_shard).await.unwrap_or(false) {
                let prefix = format!("{}#", dst_key_hash);
                let mut entries = fs::read_dir(&dst_shard)
                    .await
                    .map_err(|e| AppError::Io(e, "read dest shard".into()))?;
                while let Ok(Some(entry)) = entries.next_entry().await {
                    let fname = entry.file_name().to_string_lossy().to_string();
                    if fname.starts_with(&prefix) && fname.ends_with(METADATA_EXT) {
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

            let src_prefix = format!("{}#", src_key_hash);
            let mut entries = fs::read_dir(&src_shard)
                .await
                .map_err(|e| AppError::Io(e, "read src shard".into()))?;

            let mut moved_versions: Vec<(String, i64, ObjectMetadata)> = Vec::new();

            while let Ok(Some(entry)) = entries.next_entry().await {
                let name = entry.file_name().to_string_lossy().to_string();

                if !name.starts_with(&src_prefix) || !name.ends_with(METADATA_EXT) {
                    continue;
                }

                let Some((_, version_ext)) = name.split_once('#') else {
                    continue;
                };
                let Some(version_id) = version_ext.strip_suffix(METADATA_EXT) else {
                    continue;
                };

                let src_path = entry.path();
                let dest_filename = format!("{}#{}{}", dst_key_hash, version_id, METADATA_EXT);
                let dest_path = dst_shard.join(&dest_filename);

                let data = match fs::read(&src_path).await {
                    Ok(d) => d,
                    Err(e) => {
                        warn!(
                            "Failed to read src meta during rename {:?}: {}",
                            src_path, e
                        );
                        continue;
                    }
                };

                let mut meta = match rmp_serde::from_slice::<ObjectMetadata>(&data) {
                    Ok(m) => m,
                    Err(e) => {
                        warn!("Failed to decode src meta {:?}: {}", src_path, e);
                        continue;
                    }
                };

                meta.object_key = payload.destination_key.clone();
                meta.fragment = payload.destination_fragment.clone();
                meta.owner_uid = payload.destination_fragment.clone();

                let bytes = match rmp_serde::to_vec(&meta) {
                    Ok(b) => b,
                    Err(e) => {
                        warn!("Failed to serialize renamed meta {:?}: {}", src_path, e);
                        continue;
                    }
                };

                if let Err(e) = write_atomic(&dest_path, &bytes).await {
                    error!(
                        "Failed to write dest meta during rename {:?}: {}",
                        dest_path, e
                    );
                    continue;
                }

                if let Some(parent) = dest_path.parent() {
                    let _ = sync_dir(parent).await;
                }

                moved_versions.push((version_id.to_string(), meta.created_at, meta));
            }

            Ok::<Vec<(String, i64, ObjectMetadata)>, AppError>(moved_versions)
        }
    });

    let results = join_all(rename_futures).await;

    let mut affected_versions: HashSet<String> = HashSet::new();
    let mut created_at_by_version: HashMap<String, i64> = HashMap::new();
    let mut template_meta: Option<ObjectMetadata> = None;
    let mut success = false;

    for result in results {
        match result {
            Ok(versions) => {
                if !versions.is_empty() {
                    success = true;
                    for (v, created_at, meta) in versions {
                        affected_versions.insert(v.clone());
                        created_at_by_version.entry(v).or_insert(created_at);
                        if template_meta.is_none() {
                            template_meta = Some(meta);
                        }
                    }
                }
            }
            Err(e) => return Err(e),
        }
    }

    if !success {
        return Err(AppError::NotFound);
    }

    for version_id in &affected_versions {
        let created_at = created_at_by_version
            .get(version_id)
            .copied()
            .unwrap_or_else(|| Utc::now().timestamp());

        write_prefix_segments_for_meta(
            &path_manager,
            &payload_arc.destination_fragment,
            &payload_arc.destination_key,
            version_id,
            created_at,
        )
        .await?;
    }

    if let Some(tmpl) = template_meta.as_ref() {
        let (del_vid, del_created_at) = write_delete_marker_for_key(
            &path_manager,
            &payload_arc.source_fragment,
            &payload_arc.source_key,
            tmpl,
        )
        .await?;

        write_prefix_segments_for_meta(
            &path_manager,
            &payload_arc.source_fragment,
            &payload_arc.source_key,
            &del_vid,
            del_created_at,
        )
        .await?;
    }

    if payload_arc.source_fragment != payload_arc.destination_fragment {
        let blob_roots = path_manager.get_blob_roots();

        for root in blob_roots {
            for version_id in &affected_versions {
                let src_blob_dir = path_manager.get_shard_dir_on_disk(
                    &root,
                    &payload_arc.source_fragment,
                    version_id,
                );

                let dst_blob_dir = path_manager.get_shard_dir_on_disk(
                    &root,
                    &payload_arc.destination_fragment,
                    version_id,
                );

                if fs::try_exists(&src_blob_dir).await.unwrap_or(false) {
                    if let Some(parent) = dst_blob_dir.parent() {
                        let _ = fs::create_dir_all(parent).await;
                    }

                    if let Err(e) = fs::rename(&src_blob_dir, &dst_blob_dir).await {
                        warn!(
                            "Failed to rename blob dir {:?} -> {:?}: {}",
                            src_blob_dir, dst_blob_dir, e
                        );
                    } else if let Some(parent) = dst_blob_dir.parent() {
                        let _ = sync_dir(parent).await;
                    }
                }
            }
        }
    }

    delete_source_metadata_versions(
        &path_manager,
        &payload_arc.source_fragment,
        &payload_arc.source_key,
        &affected_versions,
    )
    .await;

    let _ = rebuild_prefix_indexes_for_key(
        &path_manager,
        &payload_arc.source_fragment,
        &payload_arc.source_key,
    )
    .await;

    let _ = rebuild_prefix_indexes_for_key(
        &path_manager,
        &payload_arc.destination_fragment,
        &payload_arc.destination_key,
    )
    .await;

    info!(
        "Renamed object from {}/{} to {}/{}",
        payload_arc.source_fragment,
        payload_arc.source_key,
        payload_arc.destination_fragment,
        payload_arc.destination_key
    );

    state.emit(
        &payload_arc.source_fragment,
        StorageEventPayload::FileRenamed {
            source_fragment: payload_arc.source_fragment.clone(),
            source_key: payload_arc.source_key.clone(),
            destination_fragment: payload_arc.destination_fragment.clone(),
            destination_key: payload_arc.destination_key.clone(),
        },
    );

    Ok(StatusCode::OK)
}
