use crate::enums::app_error_enum::AppError;
use crate::fs::cleanup_manager::start_cleanup;
use crate::fs::fs_constants::STORAGE_ROOT;
use crate::object_storage::constants::{OBJECT_DATA_ROOT, PART_META_SUFFIX, QUOTA_FILE};
use crate::object_storage::quota_manager::update_quota;
use crate::routes::storage_routes::AppState;
use crate::settings::config::CONFIG;
use crate::structs::object_storage_structs::{
    BLOB_STORE_DISKS, FS_STORAGE_ROOT, INDEX_STORE_DISKS, ObjectMetadata, ReservationReceipt,
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

#[derive(Default)]
struct GcCounters {
    gc_files_removed: u64,
    gc_dirs_removed: u64,
    user_frags_seen: u64,
    meta_files_seen: u64,
    meta_parsed_ok: u64,
    trash_entries_purged: u64,
    trash_objects_purged: u64,
    meta_deleted: u64,
    blob_versions_deleted: u64,
    reservation_orphans_deleted: u64,
    quota_refunds: u64,
    quota_refund_bytes: u64,
}

pub async fn run_lifecycle_manager(state: Arc<AppState>) {
    let cleanup_interval = Duration::from_secs(CONFIG.garbage_collector.cleanup_interval_seconds);
    let retention = Duration::from_secs(CONFIG.garbage_collector.retention_seconds);
    let ttl = Duration::from_secs(CONFIG.garbage_collector.file_ttl_seconds);

    info!(
        "GC lifecycle started (interval={:?}, retention={:?}, ttl={:?})",
        cleanup_interval, retention, ttl
    );

    loop {
        let root = FS_STORAGE_ROOT
            .get()
            .map(|s| s.as_str())
            .unwrap_or(STORAGE_ROOT);

        let started = std::time::Instant::now();
        match perform_gc_scan(state.clone(), false).await {
            Ok(_) => info!("GC lifecycle scan finished in {:?}.", started.elapsed()),
            Err(err) => error!("GC lifecycle scan failed: {}", err),
        }

        start_cleanup(root).await;
        sleep(cleanup_interval).await;
    }
}

pub async fn run_garbage_collector(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    tokio::spawn(async move {
        let started = std::time::Instant::now();

        if let Err(err) = perform_gc_scan(state.clone(), true).await {
            error!("Manual GC scan failed: {}", err);
        } else {
            info!("Manual GC scan finished in {:?}.", started.elapsed());
        }

        let root = FS_STORAGE_ROOT
            .get()
            .map(|s| s.as_str())
            .unwrap_or(STORAGE_ROOT);
        start_cleanup(root).await;
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

fn created_system_time(created_at: i64) -> Option<SystemTime> {
    if created_at <= 0 {
        return None;
    }
    let v = created_at as u128;
    if v > 10_000_000_000 {
        Some(SystemTime::UNIX_EPOCH + Duration::from_millis(v as u64))
    } else {
        Some(SystemTime::UNIX_EPOCH + Duration::from_secs(v as u64))
    }
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

fn remove_file_strict(path: &Path) -> bool {
    let parent = match path.parent() {
        Some(p) => p,
        None => return false,
    };

    if !path.exists() {
        return false;
    }

    if path.extension().and_then(|e| e.to_str()) == Some("gc") {
        let ok = fs::remove_file(path).is_ok();
        sync_dir_std(parent);
        return ok;
    }

    let gc_path = PathBuf::from(format!("{}.gc", path.to_string_lossy()));
    if !gc_path.exists() {
        if fs::rename(path, &gc_path).is_ok() {
            sync_dir_std(parent);
        } else {
            return false;
        }
    }

    let ok = fs::remove_file(&gc_path).is_ok();
    sync_dir_std(parent);
    ok
}

fn remove_dir_strict(dir: &Path) -> bool {
    let parent = match dir.parent() {
        Some(p) => p,
        None => return false,
    };

    if !dir.exists() {
        return false;
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
                return false;
            }
        }
        gc_dir
    };

    let ok = fs::remove_dir_all(&target).is_ok();
    sync_dir_std(parent);
    ok
}

#[inline]
fn is_gc_file(p: &Path) -> bool {
    p.extension().and_then(|e| e.to_str()) == Some("gc")
}

#[inline]
fn is_gc_dir_name(name: &str) -> bool {
    name.ends_with(".gcdir")
}

fn clean_gc_artifacts_recursive(root: &Path, counters: &mut GcCounters) {
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
                if remove_file_strict(p) {
                    counters.gc_files_removed += 1;
                }
            }
            continue;
        }

        if entry.file_type().is_dir() {
            if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
                if is_gc_dir_name(name) {
                    if remove_dir_strict(p) {
                        counters.gc_dirs_removed += 1;
                    }
                }
            }
        }
    }
}

fn clean_gc_artifacts_in_versions_dir(versions_root: &Path, counters: &mut GcCounters) {
    if !versions_root.exists() {
        return;
    }

    if let Ok(entries) = fs::read_dir(versions_root) {
        for e in entries.filter_map(|e| e.ok()) {
            let p = e.path();
            let name = e.file_name().to_string_lossy().to_string();

            if p.is_file() && name.ends_with(".gc") {
                if remove_file_strict(&p) {
                    counters.gc_files_removed += 1;
                }
                continue;
            }

            if p.is_dir() && name.ends_with(".gcdir") {
                if remove_dir_strict(&p) {
                    counters.gc_dirs_removed += 1;
                }
                continue;
            }
        }
    }
}

fn dir_age(now: SystemTime, p: &Path) -> Duration {
    fs::metadata(p)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|mt| now.duration_since(mt).ok())
        .unwrap_or(Duration::ZERO)
}

fn read_reservation_amount(version_dir: &Path) -> Option<u64> {
    let receipt_path = version_dir.join("reservation.bson");
    if !receipt_path.exists() {
        return None;
    }
    let data = fs::read(receipt_path).ok()?;
    let r = mongodb::bson::from_slice::<ReservationReceipt>(&data).ok()?;
    if r.amount > 0 { Some(r.amount) } else { None }
}

fn purge_fs_trash_dir(
    trash_dir: &Path,
    now: SystemTime,
    retention: Duration,
    counters: &mut GcCounters,
) {
    if !trash_dir.exists() {
        return;
    }

    let rd = match fs::read_dir(trash_dir) {
        Ok(r) => r,
        Err(_) => return,
    };

    for e in rd.filter_map(|e| e.ok()) {
        let p = e.path();
        let age = dir_age(now, &p);

        if age <= retention {
            continue;
        }

        let ft = match e.file_type() {
            Ok(t) => t,
            Err(_) => continue,
        };

        if ft.is_dir() {
            if remove_dir_strict(&p) {
                counters.trash_entries_purged += 1;
            }
        } else if ft.is_file() {
            if remove_file_strict(&p) {
                counters.trash_entries_purged += 1;
            }
        }
    }
}

fn cleanup_blobs_under(
    blobs_root: &Path,
    referenced_versions: &HashSet<String>,
    purge_versions: &HashSet<String>,
    now: SystemTime,
    ttl: Duration,
    uid: &str,
    state: &Arc<AppState>,
    counters: &mut GcCounters,
) {
    if !blobs_root.exists() {
        return;
    }

    clean_gc_artifacts_in_versions_dir(blobs_root, counters);

    let rd = match fs::read_dir(blobs_root) {
        Ok(r) => r,
        Err(_) => return,
    };

    for e in rd.filter_map(|e| e.ok()) {
        let vdir = e.path();
        if !vdir.is_dir() {
            continue;
        }

        let ver = e.file_name().to_string_lossy().to_string();

        if ver.ends_with(".gcdir") {
            if remove_dir_strict(&vdir) {
                counters.gc_dirs_removed += 1;
            }
            continue;
        }

        if purge_versions.contains(&ver) {
            if remove_dir_strict(&vdir) {
                counters.blob_versions_deleted += 1;
            }
            continue;
        }

        if referenced_versions.contains(&ver) {
            continue;
        }

        if let Some(amount) = read_reservation_amount(&vdir) {
            let age = dir_age(now, &vdir);
            if age > ttl {
                if remove_dir_strict(&vdir) {
                    counters.reservation_orphans_deleted += 1;
                    counters.quota_refunds += 1;
                    counters.quota_refund_bytes += amount;
                    refund_quota_async(uid.to_string(), amount, state.clone());
                }
            }
        }
    }
}

async fn perform_gc_scan(state: Arc<AppState>, is_manual_run: bool) -> Result<(), AppError> {
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

    let reachable_index: Vec<PathBuf> =
        index_roots.iter().cloned().filter(|p| p.exists()).collect();
    let reachable_blob: Vec<PathBuf> = blob_roots.iter().cloned().filter(|p| p.exists()).collect();

    if reachable_index.is_empty() && reachable_blob.is_empty() {
        info!("GC: no reachable roots (manual={}).", is_manual_run);
        return Ok(());
    }

    let retention = Duration::from_secs(CONFIG.garbage_collector.retention_seconds);
    let ttl = Duration::from_secs(CONFIG.garbage_collector.file_ttl_seconds);
    let now = SystemTime::now();
    let state_clone = state.clone();

    info!(
        "GC: start (manual={}, reachable_index_roots={}, reachable_blob_roots={}, retention_seconds={}, ttl_seconds={})",
        is_manual_run,
        reachable_index.len(),
        reachable_blob.len(),
        CONFIG.garbage_collector.retention_seconds,
        CONFIG.garbage_collector.file_ttl_seconds
    );

    let counters = tokio::task::spawn_blocking(move || -> Result<GcCounters, AppError> {
        let mut counters = GcCounters::default();

        for r in &reachable_index {
            clean_gc_artifacts_recursive(r, &mut counters);
        }
        for r in &reachable_blob {
            clean_gc_artifacts_recursive(r, &mut counters);
        }

        let mut user_frags: HashSet<(String, String)> = HashSet::new();

        for root in &reachable_index {
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
                    if fe.path().is_dir() {
                        let frag = fe.file_name().to_string_lossy().to_string();
                        user_frags.insert((uid.clone(), frag));
                    }
                }
            }
        }

        for root in &reachable_blob {
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
                    if fe.path().is_dir() {
                        let frag = fe.file_name().to_string_lossy().to_string();
                        user_frags.insert((uid.clone(), frag));
                    }
                }
            }
        }

        for (uid, fragment) in user_frags {
            counters.user_frags_seen += 1;

            for root in &reachable_index {
                let trash_dir = root.join(&uid).join(&fragment).join(".trash");
                purge_fs_trash_dir(&trash_dir, now, retention, &mut counters);
            }
            for root in &reachable_blob {
                let trash_dir = root.join(&uid).join(&fragment).join(".trash");
                purge_fs_trash_dir(&trash_dir, now, retention, &mut counters);
            }

            let mut referenced_versions: HashSet<String> = HashSet::new();
            let mut obj_map: HashMap<String, Vec<(String, PathBuf)>> = HashMap::new();

            for root in &reachable_index {
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

                    let Some((hash, ver)) = parse_meta_filename(name) else {
                        continue;
                    };

                    counters.meta_files_seen += 1;
                    referenced_versions.insert(ver.to_string());
                    obj_map
                        .entry(hash.to_string())
                        .or_default()
                        .push((ver.to_string(), p.to_path_buf()));
                }
            }

            let mut purge_versions: HashSet<String> = HashSet::new();

            for (_hash, vers) in &mut obj_map {
                if vers.is_empty() {
                    continue;
                }

                vers.sort_by(|a, b| a.0.cmp(&b.0));
                let (_head_ver, head_path) = match vers.last().cloned() {
                    Some(v) => v,
                    None => continue,
                };

                let meta = match fs::read(&head_path)
                    .ok()
                    .and_then(|data| mongodb::bson::from_slice::<ObjectMetadata>(&data).ok())
                {
                    Some(m) => {
                        counters.meta_parsed_ok += 1;
                        m
                    }
                    None => continue,
                };

                let head_is_trash = meta.is_delete_marker || is_trash_key(&meta.object_key);
                if !head_is_trash {
                    continue;
                }

                let created = created_system_time(meta.created_at)
                    .or_else(|| {
                        fs::metadata(&head_path)
                            .ok()
                            .and_then(|m| m.modified().ok())
                    })
                    .unwrap_or(now);

                let age = now.duration_since(created).unwrap_or(Duration::ZERO);
                if age <= retention {
                    continue;
                }

                counters.trash_objects_purged += 1;
                for (ver, _p) in vers.iter() {
                    purge_versions.insert(ver.clone());
                }
            }

            if !purge_versions.is_empty() {
                for root in &reachable_index {
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

                        if purge_versions.contains(ver) {
                            if remove_file_strict(p) {
                                counters.meta_deleted += 1;
                            }
                        }
                    }
                }
            }

            for root in &reachable_index {
                let blobs_root = root.join(&uid).join(&fragment).join("blobs");
                cleanup_blobs_under(
                    &blobs_root,
                    &referenced_versions,
                    &purge_versions,
                    now,
                    ttl,
                    &uid,
                    &state_clone,
                    &mut counters,
                );
            }

            for root in &reachable_blob {
                let blobs_root = root.join(&uid).join(&fragment).join("blobs");
                cleanup_blobs_under(
                    &blobs_root,
                    &referenced_versions,
                    &purge_versions,
                    now,
                    ttl,
                    &uid,
                    &state_clone,
                    &mut counters,
                );
            }
        }

        for r in &reachable_index {
            clean_gc_artifacts_recursive(r, &mut counters);
        }
        for r in &reachable_blob {
            clean_gc_artifacts_recursive(r, &mut counters);
        }

        Ok(counters)
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("GC Task Failed: {}", e)))??;

    info!(
        "GC: done (manual={}, user_frags={}, meta_seen={}, meta_ok={}, trash_entries_purged={}, trash_objects_purged={}, meta_deleted={}, blob_deleted={}, reservation_orphans_deleted={}, refunds={}, refund_bytes={}, gc_files_removed={}, gc_dirs_removed={})",
        is_manual_run,
        counters.user_frags_seen,
        counters.meta_files_seen,
        counters.meta_parsed_ok,
        counters.trash_entries_purged,
        counters.trash_objects_purged,
        counters.meta_deleted,
        counters.blob_versions_deleted,
        counters.reservation_orphans_deleted,
        counters.quota_refunds,
        counters.quota_refund_bytes,
        counters.gc_files_removed,
        counters.gc_dirs_removed
    );

    Ok(())
}
