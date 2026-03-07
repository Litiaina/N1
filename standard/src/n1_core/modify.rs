use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use axum::extract::State;
use axum::{Json as AxumJson, http::StatusCode, response::IntoResponse};
use chrono::Utc;
use futures_util::future::join_all;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::{info, warn};
use ulid::Ulid;

use crate::enums::app_error_enum::AppError;
use crate::n1_core::disk_manager::{
    ObjectMetadata, ObjectPathManager, PrefixIndexEntry, PrefixIndexSegment, RenameObjectRequest,
};
use crate::n1_core::index;
use crate::n1_core::prefix_compactor::schedule_prefix_compaction;
use crate::n1_core::prefix_delimiter::write_delimiter_delta;
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

    match fs::rename(&tmp, path).await {
        Ok(_) => {}
        Err(e) => {
            if matches!(
                e.kind(),
                std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::PermissionDenied
            ) {
                let _ = fs::remove_file(path).await;
                fs::rename(&tmp, path).await.map_err(|e| {
                    AppError::Io(e, format!("rename {} -> {}", tmp.display(), path.display()))
                })?;
            } else {
                return Err(AppError::Io(
                    e,
                    format!("rename {} -> {}", tmp.display(), path.display()),
                ));
            }
        }
    }

    sync_dir(parent).await?;
    Ok(())
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

async fn write_prefix_segments_for_meta(
    path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
    version_id: &str,
    created_at: i64,
    is_delete_marker: bool,
    child_count_delta: i64,
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
                is_delete_marker,
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
                match write_atomic(&p, &d).await {
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

    write_delimiter_delta(
        path_manager,
        fragment,
        object_key,
        version_id,
        Some(PrefixIndexEntry {
            object_key: object_key.to_string(),
            version_id: version_id.to_string(),
            created_at,
            is_delete_marker,
        }),
        child_count_delta,
    )
    .await?;

    Ok(())
}

async fn invalidate_compacted_for_key(
    _path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
) {
    let prefixes = prefixes_for_key(object_key);
    let mut compact_candidates = Vec::new();
    for pfx in prefixes {
        let cpfx = canonical_prefix(&pfx);
        compact_candidates.push(cpfx.clone());
    }
    schedule_prefix_compaction(fragment.to_string(), compact_candidates);
}

async fn write_delete_marker_for_key(
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

    index::publish_object(&dm).await?;

    Ok((delete_version_id, now))
}

fn remap_shard_absolute_path_for_fragment_move(
    path_manager: &ObjectPathManager,
    blob_roots: &[std::path::PathBuf],
    source_fragment: &str,
    destination_fragment: &str,
    version_id: &str,
    current_path: &str,
    file_name: &str,
) -> String {
    for root in blob_roots {
        let src_dir = path_manager.get_shard_dir_on_disk(root, source_fragment, version_id);
        let src_dir_s = src_dir.to_string_lossy();
        if current_path.starts_with(src_dir_s.as_ref()) {
            return path_manager
                .get_shard_dir_on_disk(root, destination_fragment, version_id)
                .join(file_name)
                .to_string_lossy()
                .to_string();
        }
    }

    let unix_from = format!("/{}/blob/{}/", source_fragment, version_id);
    let unix_to = format!("/{}/blob/{}/", destination_fragment, version_id);
    if current_path.contains(&unix_from) {
        return current_path.replacen(&unix_from, &unix_to, 1);
    }

    let win_from = format!("\\{}\\blob\\{}\\", source_fragment, version_id);
    let win_to = format!("\\{}\\blob\\{}\\", destination_fragment, version_id);
    if current_path.contains(&win_from) {
        return current_path.replacen(&win_from, &win_to, 1);
    }

    current_path.to_string()
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
    if payload.source_fragment == payload.destination_fragment
        && payload.source_key == payload.destination_key
    {
        return Ok(StatusCode::OK);
    }

    let destination_versions = index::list_live_object_versions_metadata(
        &payload.destination_fragment,
        &payload.destination_key,
    )
    .await?;
    if destination_versions.iter().any(|m| !m.is_delete_marker) {
        return Err(AppError::Conflict(
            "Destination object already exists".into(),
        ));
    }

    let source_versions =
        index::list_live_object_versions_metadata(&payload.source_fragment, &payload.source_key)
            .await?;
    if source_versions.is_empty() {
        return Err(AppError::NotFound);
    }

    let blob_roots = path_manager.get_blob_roots();
    let mut affected_versions: HashSet<String> = HashSet::new();
    let mut created_at_by_version: HashMap<String, i64> = HashMap::new();
    let mut template_meta: Option<ObjectMetadata> = None;
    for source_meta in source_versions {
        if source_meta.is_delete_marker {
            continue;
        }
        let mut new_meta = source_meta.clone();
        new_meta.object_key = payload.destination_key.clone();
        new_meta.fragment = payload.destination_fragment.clone();
        new_meta.owner_uid = payload.destination_fragment.clone();
        new_meta.created_at = Utc::now().timestamp();

        if payload.source_fragment != payload.destination_fragment {
            for part in &mut new_meta.parts {
                for shard in &mut part.shards {
                    shard.absolute_path = remap_shard_absolute_path_for_fragment_move(
                        &path_manager,
                        &blob_roots,
                        &payload.source_fragment,
                        &payload.destination_fragment,
                        &new_meta.version_id,
                        &shard.absolute_path,
                        &shard.file_name,
                    );
                }
            }
        }

        index::upsert_object_version(&new_meta).await?;
        affected_versions.insert(new_meta.version_id.clone());
        created_at_by_version
            .entry(new_meta.version_id.clone())
            .or_insert(new_meta.created_at);
        if template_meta.is_none() {
            template_meta = Some(source_meta);
        }
    }

    if affected_versions.is_empty() {
        return Err(AppError::NotFound);
    }

    for version_id in &affected_versions {
        let created_at = created_at_by_version
            .get(version_id)
            .copied()
            .unwrap_or_else(|| Utc::now().timestamp());

        write_prefix_segments_for_meta(
            &path_manager,
            &payload.destination_fragment,
            &payload.destination_key,
            version_id,
            created_at,
            false,
            1,
        )
        .await?;
    }

    for version_id in &affected_versions {
        let _ =
            index::delete_object_version(&payload.source_fragment, &payload.source_key, version_id)
                .await;
    }

    if let Some(tmpl) = template_meta.as_ref() {
        let (del_vid, del_created_at) =
            write_delete_marker_for_key(&payload.source_fragment, &payload.source_key, tmpl)
                .await?;

        write_prefix_segments_for_meta(
            &path_manager,
            &payload.source_fragment,
            &payload.source_key,
            &del_vid,
            del_created_at,
            true,
            -1,
        )
        .await?;
    }

    if payload.source_fragment != payload.destination_fragment {
        for root in blob_roots {
            for version_id in &affected_versions {
                let src_blob_dir =
                    path_manager.get_shard_dir_on_disk(&root, &payload.source_fragment, version_id);

                let dst_blob_dir = path_manager.get_shard_dir_on_disk(
                    &root,
                    &payload.destination_fragment,
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

    invalidate_compacted_for_key(&path_manager, &payload.source_fragment, &payload.source_key)
        .await;

    invalidate_compacted_for_key(
        &path_manager,
        &payload.destination_fragment,
        &payload.destination_key,
    )
    .await;

    info!(
        "Renamed object from {}/{} to {}/{}",
        payload.source_fragment,
        payload.source_key,
        payload.destination_fragment,
        payload.destination_key
    );

    state.emit(
        &payload.source_fragment,
        StorageEventPayload::FileRenamed {
            source_fragment: payload.source_fragment.clone(),
            source_key: payload.source_key.clone(),
            destination_fragment: payload.destination_fragment.clone(),
            destination_key: payload.destination_key.clone(),
        },
    );

    Ok(StatusCode::OK)
}
