use crate::enums::app_error_enum::AppError;
use crate::fs::cleanup_manager::start_cleanup;
use crate::fs::fs_constants::STORAGE_ROOT;
use crate::object_storage::constants::{OBJECT_DATA_ROOT, PART_META_SUFFIX, QUOTA_FILE};
use crate::object_storage::quota_manager::update_quota;
use crate::routes::storage_routes::AppState;
use crate::settings::config::CONFIG;
use crate::structs::object_storage_structs::{
    BLOB_STORE_DISKS, FS_STORAGE_ROOT, INDEX_STORE_DISKS, ObjectMetadata,
};
use crate::websocket::ws_structs::StorageEventPayload;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::runtime::Handle;
use tokio::time::sleep;

use tracing::{error, info, warn};
use walkdir::WalkDir;

pub async fn run_lifecycle_manager(state: Arc<AppState>) {
    let cleanup_interval = Duration::from_secs(CONFIG.garbage_collector.cleanup_interval_seconds);

    loop {
        if let Err(err) = perform_gc_scan(state.clone(), false).await {
            error!("GC scan failed: {}", err);
        }
        sleep(cleanup_interval).await;
    }
}

pub async fn run_garbage_collector(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    tokio::spawn(async move {
        if let Err(err) = perform_gc_scan(state.clone(), true).await {
            error!("Manual GC scan failed: {}", err);
        }

        start_cleanup(FS_STORAGE_ROOT.get().unwrap_or(&STORAGE_ROOT.to_string())).await;
    });

    StatusCode::ACCEPTED
}

fn refund_quota_async(uid: String, amount: u64, state: Arc<AppState>) {
    if amount == 0 {
        return;
    }

    let handle = Handle::current();
    handle.spawn(async move {
        if let Err(e) = update_quota(&uid, -(amount as i64)).await {
            warn!("GC: Failed to refund quota for user {}: {}", uid, e);
        } else {
            info!("GC: Refunded {} bytes to user {}", amount, uid);
            state.emit(&uid, StorageEventPayload::StateInvalidated);
        }
    });
}

#[inline]
fn is_trash_key(object_key: &str) -> bool {
    object_key == ".trash" || object_key.starts_with(".trash/")
}

#[inline]
fn parse_meta_filename(name: &str) -> Option<(&str, &str)> {
    if !name.ends_with(".bson") {
        return None;
    }
    let (hash, ver_ext) = name.split_once('#')?;
    let ver = ver_ext.strip_suffix(".bson")?;
    if hash.is_empty() || ver.is_empty() {
        return None;
    }
    Some((hash, ver))
}

#[cfg(unix)]
fn sync_dir_std(path: &Path) {
    use std::fs::OpenOptions;
    use std::os::unix::fs::OpenOptionsExt;

    if let Ok(f) = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY)
        .open(path)
    {
        let _ = f.sync_all();
    }
}

#[cfg(windows)]
fn sync_dir_std(path: &Path) {
    use std::fs::OpenOptions;
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x02000000;

    if let Ok(f) = OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
    {
        let _ = f.sync_all();
    }
}

#[cfg(not(any(unix, windows)))]
fn sync_dir_std(_path: &Path) {}

fn remove_file_strict(path: &Path) {
    let parent = match path.parent() {
        Some(p) => p,
        None => return,
    };

    if !path.exists() {
        return;
    }

    if path.extension().and_then(|e| e.to_str()) == Some("gc") {
        let _ = fs::remove_file(path);
        sync_dir_std(parent);
        return;
    }

    let gc_path = PathBuf::from(format!("{}.gc", path.to_string_lossy()));
    if !gc_path.exists() {
        if fs::rename(path, &gc_path).is_ok() {
            sync_dir_std(parent);
        } else {
            return;
        }
    }

    let _ = fs::remove_file(&gc_path);
    sync_dir_std(parent);
}

fn remove_dir_strict(dir: &Path) {
    let parent = match dir.parent() {
        Some(p) => p,
        None => return,
    };

    if !dir.exists() {
        return;
    }

    let is_gcdir = dir
        .file_name()
        .and_then(|n| n.to_str())
        .map(|s| s.ends_with(".gcdir"))
        .unwrap_or(false);

    let target = if is_gcdir {
        dir.to_path_buf()
    } else {
        let gc_dir = PathBuf::from(format!("{}.gcdir", dir.to_string_lossy()));
        if !gc_dir.exists() {
            if fs::rename(dir, &gc_dir).is_ok() {
                sync_dir_std(parent);
            } else {
                return;
            }
        }
        gc_dir
    };

    let _ = fs::remove_dir_all(&target);
    sync_dir_std(parent);
}

#[inline]
fn is_gc_file(p: &Path) -> bool {
    p.extension().and_then(|e| e.to_str()) == Some("gc")
}

#[inline]
fn is_gc_dir_name(name: &str) -> bool {
    name.ends_with(".gcdir")
}

fn clean_gc_artifacts_recursive(root: &Path) {
    if !root.exists() {
        return;
    }

    for entry in WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let p = entry.path();

        if entry.file_type().is_file() {
            if is_gc_file(p) {
                remove_file_strict(p);
            }
            continue;
        }

        if entry.file_type().is_dir() {
            if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
                if is_gc_dir_name(name) {
                    remove_dir_strict(p);
                }
            }
        }
    }
}

fn clean_gc_artifacts_in_versions_dir(versions_root: &Path) {
    if !versions_root.exists() {
        return;
    }

    if let Ok(entries) = fs::read_dir(versions_root) {
        for e in entries.filter_map(|e| e.ok()) {
            let p = e.path();
            let name = e.file_name().to_string_lossy().to_string();

            if p.is_file() && name.ends_with(".gc") {
                remove_file_strict(&p);
                continue;
            }

            if p.is_dir() && name.ends_with(".gcdir") {
                remove_dir_strict(&p);
                continue;
            }
        }
    }
}

async fn perform_gc_scan(state: Arc<AppState>, _is_manual_run: bool) -> Result<(), AppError> {
    let index_roots_cfg = INDEX_STORE_DISKS
        .get()
        .ok_or(AppError::InternalServerError(
            "Index Store Config not initialized".into(),
        ))?;

    let blob_roots_cfg = BLOB_STORE_DISKS.get().ok_or(AppError::InternalServerError(
        "Blob Store Config not initialized".into(),
    ))?;

    let index_roots: Vec<PathBuf> = index_roots_cfg
        .iter()
        .map(|s| PathBuf::from(s).join(OBJECT_DATA_ROOT))
        .collect();

    let blob_roots: Vec<PathBuf> = blob_roots_cfg
        .iter()
        .map(|s| PathBuf::from(s).join(OBJECT_DATA_ROOT))
        .collect();

    if !index_roots.iter().any(|p| p.exists()) && !blob_roots.iter().any(|p| p.exists()) {
        return Ok(());
    }

    let retention = Duration::from_secs(CONFIG.garbage_collector.retention_days * 86400);
    let now = SystemTime::now();
    let state_clone = state.clone();

    tokio::task::spawn_blocking(move || -> Result<(), AppError> {
        for root in &index_roots {
            clean_gc_artifacts_recursive(root);
        }
        for root in &blob_roots {
            clean_gc_artifacts_recursive(root);
        }

        let mut user_frags: HashSet<(String, String)> = HashSet::new();

        for root in &index_roots {
            if !root.exists() {
                continue;
            }
            let users = match fs::read_dir(root) {
                Ok(u) => u,
                Err(_) => continue,
            };

            for ue in users.filter_map(|e| e.ok()) {
                let uid = ue.file_name().to_string_lossy().to_string();
                let up = ue.path();
                if !up.is_dir() {
                    continue;
                }

                let frags = match fs::read_dir(&up) {
                    Ok(f) => f,
                    Err(_) => continue,
                };

                for fe in frags.filter_map(|e| e.ok()) {
                    let frag = fe.file_name().to_string_lossy().to_string();
                    if fe.path().is_dir() {
                        user_frags.insert((uid.clone(), frag));
                    }
                }
            }
        }

        for root in &blob_roots {
            if !root.exists() {
                continue;
            }
            let users = match fs::read_dir(root) {
                Ok(u) => u,
                Err(_) => continue,
            };

            for ue in users.filter_map(|e| e.ok()) {
                let uid = ue.file_name().to_string_lossy().to_string();
                let up = ue.path();
                if !up.is_dir() {
                    continue;
                }

                let frags = match fs::read_dir(&up) {
                    Ok(f) => f,
                    Err(_) => continue,
                };

                for fe in frags.filter_map(|e| e.ok()) {
                    let frag = fe.file_name().to_string_lossy().to_string();
                    if fe.path().is_dir() {
                        user_frags.insert((uid.clone(), frag));
                    }
                }
            }
        }

        for (uid, fragment) in user_frags {
            let mut versions: HashMap<String, HashMap<String, PathBuf>> = HashMap::new();
            let mut version_sizes: HashMap<String, u64> = HashMap::new();
            let mut valid_versions: HashSet<String> = HashSet::new();

            for root in &index_roots {
                let idx = root.join(&uid).join(&fragment).join("index");
                if !idx.exists() {
                    continue;
                }

                for entry in WalkDir::new(&idx).into_iter().filter_map(|e| e.ok()) {
                    if !entry.file_type().is_file() {
                        continue;
                    }

                    let p = entry.path();
                    let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");

                    if name == QUOTA_FILE || name == "reservation.bson" {
                        continue;
                    }
                    if name.ends_with(PART_META_SUFFIX) {
                        continue;
                    }

                    let Some((hash, ver)) = parse_meta_filename(name) else {
                        continue;
                    };

                    let vmap = versions.entry(hash.to_string()).or_default();
                    vmap.entry(ver.to_string()).or_insert_with(|| p.to_path_buf());
                }
            }

            if versions.is_empty() {
                for root in &blob_roots {
                    let vroot = root.join(&uid).join(&fragment).join("blobs");
                    clean_gc_artifacts_in_versions_dir(&vroot);
                }
                for root in &index_roots {
                    let vroot = root.join(&uid).join(&fragment).join("blobs");
                    clean_gc_artifacts_in_versions_dir(&vroot);
                }
                continue;
            }

            let mut keep_versions: HashSet<String> = HashSet::new();
            let mut expire_versions: HashSet<String> = HashSet::new();

            for (_hash, vmap) in &versions {
                let mut all_vers: Vec<String> = vmap.keys().cloned().collect();
                all_vers.sort();
                let Some(head_ver) = all_vers.last().cloned() else { continue; };

                let head_path = vmap.get(&head_ver).cloned();

                let mut head_is_trash = false;
                let mut head_age = Duration::ZERO;

                if let Some(p) = head_path.as_ref() {
                    if let Ok(data) = fs::read(p) {
                        if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                            valid_versions.insert(meta.version_id.clone());
                            head_is_trash = meta.is_delete_marker || is_trash_key(&meta.object_key);
                            version_sizes.entry(head_ver.clone()).or_insert(meta.total_size_bytes);
                            let created =
                                SystemTime::UNIX_EPOCH + Duration::from_secs(meta.created_at as u64);
                            head_age = now.duration_since(created).unwrap_or(Duration::ZERO);
                        }
                    }
                }

                let purge_all = head_is_trash && head_age > retention;

                if !purge_all {
                    keep_versions.insert(head_ver.clone());
                }

                for ver in all_vers {
                    if purge_all {
                        expire_versions.insert(ver);
                        continue;
                    }

                    if ver == head_ver {
                        continue;
                    }

                    let mut age = Duration::ZERO;

                    if let Some(p) = vmap.get(&ver) {
                        if let Ok(data) = fs::read(p) {
                            if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                                valid_versions.insert(meta.version_id.clone());
                                version_sizes.entry(ver.clone()).or_insert(meta.total_size_bytes);
                                let created = SystemTime::UNIX_EPOCH
                                    + Duration::from_secs(meta.created_at as u64);
                                age = now.duration_since(created).unwrap_or(Duration::ZERO);
                            }
                        }
                    }

                    if age > retention {
                        expire_versions.insert(ver);
                    } else {
                        keep_versions.insert(ver);
                    }
                }
            }

            for root in &index_roots {
                let vroot = root.join(&uid).join(&fragment).join("blobs");
                clean_gc_artifacts_in_versions_dir(&vroot);
            }
            for root in &blob_roots {
                let vroot = root.join(&uid).join(&fragment).join("blobs");
                clean_gc_artifacts_in_versions_dir(&vroot);
            }

            if expire_versions.is_empty() {
                continue;
            }

            for root in &index_roots {
                if !root.exists() {
                    continue;
                }

                let idx_dir = root.join(&uid).join(&fragment).join("index");
                if !idx_dir.exists() {
                    continue;
                }

                for entry in WalkDir::new(&idx_dir).into_iter().filter_map(|e| e.ok()) {
                    if !entry.file_type().is_file() {
                        continue;
                    }
                    let p = entry.path();
                    let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");

                    if name == QUOTA_FILE || name == "reservation.bson" {
                        continue;
                    }
                    if name.ends_with(PART_META_SUFFIX) {
                        continue;
                    }

                    let Some((_hash, ver)) = parse_meta_filename(name) else {
                        continue;
                    };

                    if expire_versions.contains(ver) {
                        remove_file_strict(p);
                    }
                }
            }

            let mut fully_deleted: HashMap<String, bool> = HashMap::new();
            for ver in &expire_versions {
                fully_deleted.insert(ver.clone(), true);
            }

            let mut delete_versions_under = |blobs_root: &Path| {
                if !blobs_root.exists() {
                    for ver in &expire_versions {
                        fully_deleted.insert(ver.clone(), false);
                    }
                    return;
                }

                clean_gc_artifacts_in_versions_dir(blobs_root);

                for ver in &expire_versions {
                    if keep_versions.contains(ver) {
                        continue;
                    }

                    let vdir = blobs_root.join(ver);
                    if vdir.exists() {
                        remove_dir_strict(&vdir);
                    }

                    if blobs_root.join(ver).exists() {
                        fully_deleted.insert(ver.clone(), false);
                    }
                }
            };

            for root in &index_roots {
                let blobs_root = root.join(&uid).join(&fragment).join("blobs");
                delete_versions_under(&blobs_root);
            }

            for root in &blob_roots {
                let blobs_root = root.join(&uid).join(&fragment).join("blobs");
                delete_versions_under(&blobs_root);
            }

            let scrub_orphans_under = |blobs_root: &Path| {
                if !blobs_root.exists() {
                    return;
                }

                clean_gc_artifacts_in_versions_dir(blobs_root);

                let rd = match fs::read_dir(blobs_root) {
                    Ok(r) => r,
                    Err(_) => return,
                };

                for e in rd.filter_map(|e| e.ok()) {
                    let p = e.path();
                    if !p.is_dir() {
                        continue;
                    }

                    let ver = e.file_name().to_string_lossy().to_string();

                    if ver.ends_with(".gcdir") {
                        remove_dir_strict(&p);
                        continue;
                    }

                    if valid_versions.contains(&ver) {
                        continue;
                    }

                    let m = fs::metadata(&p).ok();
                    let age = m
                        .and_then(|mm| mm.modified().ok())
                        .and_then(|mt| now.duration_since(mt).ok())
                        .unwrap_or(Duration::ZERO);

                    if age > retention {
                        remove_dir_strict(&p);
                    }
                }
            };

            for root in &index_roots {
                let blobs_root = root.join(&uid).join(&fragment).join("blobs");
                scrub_orphans_under(&blobs_root);
            }

            for root in &blob_roots {
                let blobs_root = root.join(&uid).join(&fragment).join("blobs");
                scrub_orphans_under(&blobs_root);
            }

            for ver in expire_versions {
                if fully_deleted.get(&ver).copied().unwrap_or(false) {
                    let amt = *version_sizes.get(&ver).unwrap_or(&0);
                    if amt > 0 {
                        refund_quota_async(uid.clone(), amt, state_clone.clone());
                    }
                } else {
                    warn!(
                        "GC: Not refunding {} for user {} (not fully deleted on all reachable roots).",
                        ver, uid
                    );
                }
            }
        }

        for root in &index_roots {
            clean_gc_artifacts_recursive(root);
        }
        for root in &blob_roots {
            clean_gc_artifacts_recursive(root);
        }

        Ok(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("GC Task Failed: {}", e)))??;

    Ok(())
}
