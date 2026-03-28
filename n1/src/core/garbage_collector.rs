use crate::core::constants::{FRAGMENT_METADATA_FILE, GC_SUFFIX};
use crate::core::disk_manager::{FS_STORAGE_ROOT, ObjectMetadata, ObjectPathManager};
use crate::core::index;
use crate::enums::app_error_enum::AppError;
use crate::fs::cleanup_manager::start_cleanup;
use crate::fs::fs_constants::STORAGE_ROOT;
use crate::routes::storage_routes::AppState;
use crate::settings::config::CONFIG;
use crate::structs::auth_structs::Claims;
use crate::websocket::ws_structs::StorageEventPayload;
use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use chrono::Utc;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};
use tokio::runtime::Handle;
use tokio::time::sleep;
use tracing::{error, info, warn};

#[derive(Debug, Clone, Serialize, Default)]
pub struct GcJobStatus {
    pub is_running: bool,
    pub phase: String,
    pub is_manual_run: bool,
    pub total_fragments: u64,
    pub total_fragments_scanned: u64,
    pub total_live_versions_seen: u64,
    pub total_trash_versions_expired: u64,
    pub total_sessions_deleted: u64,
    pub total_orphan_blob_dirs_deleted: u64,
    pub total_refund_bytes: u64,
    pub total_errors: u64,
    pub current_fragment: String,
    pub last_error: Option<String>,
    pub start_timestamp: i64,
    pub duration_seconds: u64,
}

#[derive(Debug, Default)]
struct FragmentGcStats {
    live_versions_seen: u64,
    trash_versions_expired: u64,
    sessions_deleted: u64,
    orphan_blob_dirs_deleted: u64,
    refund_bytes: u64,
}

static GC_STATUS: std::sync::LazyLock<Arc<RwLock<GcJobStatus>>> =
    std::sync::LazyLock::new(|| Arc::new(RwLock::new(GcJobStatus::default())));
static GC_RUNNING: std::sync::LazyLock<AtomicBool> =
    std::sync::LazyLock::new(|| AtomicBool::new(false));
static GC_STOP_SIGNAL: std::sync::LazyLock<AtomicBool> =
    std::sync::LazyLock::new(|| AtomicBool::new(false));

#[inline]
fn should_stop_gc() -> bool {
    GC_STOP_SIGNAL.load(Ordering::Relaxed)
}

fn gc_status_read_clone() -> GcJobStatus {
    GC_STATUS
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

pub async fn get_gc_status() -> GcJobStatus {
    gc_status_read_clone()
}

fn with_gc_status_write<F>(f: F)
where
    F: FnOnce(&mut GcJobStatus),
{
    let mut st = GC_STATUS
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    f(&mut st);
}

async fn run_gc_scan_offthread(state: Arc<AppState>, is_manual_run: bool) -> Result<(), AppError> {
    let handle = Handle::current();
    let join =
        tokio::task::spawn_blocking(move || handle.block_on(perform_gc_scan(state, is_manual_run)))
            .await;

    match join {
        Ok(res) => res,
        Err(e) => Err(AppError::InternalServerError(format!(
            "GC worker join error: {}",
            e
        ))),
    }
}

pub async fn run_lifecycle_manager(state: Arc<AppState>) {
    let interval_secs = if CONFIG.garbage_collector.cleanup_interval_seconds > 0 {
        CONFIG.garbage_collector.cleanup_interval_seconds
    } else {
        3600
    };
    let cleanup_interval = Duration::from_secs(interval_secs);

    loop {
        if GC_RUNNING.swap(true, Ordering::SeqCst) {
            sleep(cleanup_interval).await;
            continue;
        }
        GC_STOP_SIGNAL.store(false, Ordering::Relaxed);

        if let Err(err) = run_gc_scan_offthread(state.clone(), false).await {
            error!("Lifecycle Manager Scan failed: {}", err);
            with_gc_status_write(|st| {
                st.total_errors += 1;
                st.last_error = Some(err.to_string());
                st.phase = "Failed".to_string();
            });
        }

        with_gc_status_write(|st| {
            st.is_running = false;
            if st.phase != "Failed" && st.phase != "Stopped by User" {
                st.phase = "Completed".to_string();
            }
        });
        GC_STOP_SIGNAL.store(false, Ordering::Relaxed);
        GC_RUNNING.store(false, Ordering::SeqCst);

        sleep(cleanup_interval).await;
    }
}

pub async fn get_gc_status_api(
    claims: crate::structs::auth_structs::Claims,
) -> Result<Json<GcJobStatus>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    Ok(Json(get_gc_status().await))
}

pub async fn run_garbage_collector(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if GC_RUNNING.swap(true, Ordering::SeqCst) {
        return (
            StatusCode::CONFLICT,
            "GC is already running. Check /v4/maintenance/gc/status.",
        );
    }
    GC_STOP_SIGNAL.store(false, Ordering::Relaxed);

    tokio::spawn(async move {
        {
            with_gc_status_write(|st| {
                *st = GcJobStatus {
                    is_running: true,
                    phase: "Initializing".to_string(),
                    is_manual_run: true,
                    start_timestamp: Utc::now().timestamp(),
                    ..Default::default()
                };
            });
        }

        if let Err(err) = run_gc_scan_offthread(state.clone(), true).await {
            error!("Manual GC Scan failed: {}", err);
            with_gc_status_write(|st| {
                st.total_errors += 1;
                st.last_error = Some(err.to_string());
                st.phase = "Failed".to_string();
            });
        }

        start_cleanup(FS_STORAGE_ROOT.get().unwrap_or(&STORAGE_ROOT.to_string())).await;

        with_gc_status_write(|st| {
            st.is_running = false;
            if st.phase != "Failed" && st.phase != "Stopped by User" {
                st.phase = "Completed".to_string();
            }
        });
        GC_STOP_SIGNAL.store(false, Ordering::Relaxed);
        GC_RUNNING.store(false, Ordering::SeqCst);
    });

    (StatusCode::ACCEPTED, "GC started")
}

pub async fn stop_garbage_collector(
    claims: Claims,
    State(_state): State<Arc<AppState>>,
) -> impl IntoResponse {
    if !claims.write || !claims.modify || !claims.delete {
        return (
            StatusCode::FORBIDDEN,
            "Write, Modify and Delete permission required",
        );
    }

    if !GC_RUNNING.load(Ordering::SeqCst) {
        return (StatusCode::OK, "GC is not running.");
    }

    GC_STOP_SIGNAL.store(true, Ordering::Relaxed);
    with_gc_status_write(|st| {
        if st.is_running {
            st.phase = "Stopping...".to_string();
        }
    });

    (StatusCode::OK, "Stop signal sent to GC.")
}

fn get_trash_root(disk_root: &std::path::Path, fragment_hash: &str) -> PathBuf {
    disk_root.join(fragment_hash).join(".trash")
}

fn trash_deleted_age(deleted_at: i64, now_unix: i64) -> Duration {
    Duration::from_secs(now_unix.saturating_sub(deleted_at.max(0)).max(0) as u64)
}

#[cfg(unix)]
fn gc_cleanup_version_dir_unix(
    version_dir: &Path,
    now: SystemTime,
    gc_ttl: Duration,
    is_manual: bool,
) {
    let rd = match fs::read_dir(version_dir) {
        Ok(r) => r,
        Err(_) => return,
    };

    let mut gc_files: Vec<(String, PathBuf)> = Vec::new();
    let mut preexisting_files: Vec<PathBuf> = Vec::new();
    let mut symlinks: Vec<PathBuf> = Vec::new();

    for ent in rd.filter_map(|e| e.ok()) {
        let p = ent.path();
        let name = match p.file_name().and_then(|n| n.to_str()) {
            Some(s) => s.to_string(),
            None => continue,
        };

        let md = match fs::symlink_metadata(&p) {
            Ok(m) => m,
            Err(_) => continue,
        };

        if md.file_type().is_symlink() {
            symlinks.push(p);
            continue;
        }

        if name.ends_with(GC_SUFFIX) {
            let modified = md.modified().unwrap_or(now);
            let age = now.duration_since(modified).unwrap_or(Duration::ZERO);
            let ttl_expired = is_manual || age > gc_ttl;
            let base = name.strip_suffix(GC_SUFFIX).unwrap_or(&name);
            let link_path = version_dir.join(base);
            // Strict reclaim rule: if no compatibility link points to this .gc file,
            // reclaim immediately instead of waiting for TTL.
            let orphan_gc = match fs::symlink_metadata(&link_path) {
                Ok(lmd) if lmd.file_type().is_symlink() => match fs::read_link(&link_path) {
                    Ok(tgt) => tgt.file_name().and_then(|v| v.to_str()) != Some(name.as_str()),
                    Err(_) => true,
                },
                _ => true,
            };
            if ttl_expired || orphan_gc {
                gc_files.push((name, p));
            }
            continue;
        }

        if name.contains(".preexisting.") {
            let modified = md.modified().unwrap_or(now);
            let age = now.duration_since(modified).unwrap_or(Duration::ZERO);
            if is_manual || age > gc_ttl {
                preexisting_files.push(p);
            }
            continue;
        }
    }

    for (gc_name, gc_path) in gc_files {
        let base = gc_name.strip_suffix(GC_SUFFIX).unwrap_or(&gc_name);
        let link_path = version_dir.join(base);

        let removed_gc = fs::remove_file(&gc_path).is_ok();

        if removed_gc {
            if let Ok(lmd) = fs::symlink_metadata(&link_path) {
                if lmd.file_type().is_symlink() {
                    if let Ok(tgt) = fs::read_link(&link_path) {
                        let tgt_name = tgt.file_name().and_then(|v| v.to_str()).unwrap_or("");
                        if tgt_name == gc_name {
                            let _ = fs::remove_file(&link_path);
                        }
                    } else {
                        let _ = fs::remove_file(&link_path);
                    }
                }
            }
        }
    }

    for p in preexisting_files {
        let _ = fs::remove_file(&p);
    }

    for sp in symlinks {
        let md = match fs::symlink_metadata(&sp) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if !md.file_type().is_symlink() {
            continue;
        }

        let tgt = match fs::read_link(&sp) {
            Ok(t) => t,
            Err(_) => {
                let _ = fs::remove_file(&sp);
                continue;
            }
        };

        let tgt_name = tgt.file_name().and_then(|v| v.to_str()).unwrap_or("");
        if !tgt_name.ends_with(GC_SUFFIX) {
            continue;
        }

        let tgt_path = sp.parent().unwrap_or(Path::new("")).join(tgt_name);
        if !tgt_path.exists() {
            let _ = fs::remove_file(&sp);
            continue;
        }

        let modified = md.modified().unwrap_or(now);
        let age = now.duration_since(modified).unwrap_or(Duration::ZERO);
        if !(is_manual || age > gc_ttl) {
            continue;
        }
    }
}

#[cfg(not(unix))]
fn gc_cleanup_version_dir_unix(
    _version_dir: &Path,
    _now: SystemTime,
    _gc_ttl: Duration,
    _is_manual: bool,
) {
}

async fn perform_gc_scan(state: Arc<AppState>, is_manual_run: bool) -> Result<(), AppError> {
    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();
    let blob_roots = path_manager.get_blob_roots();

    let fragments = collect_gc_fragments(&index_roots);

    info!("GC: Starting scan for {} fragments...", fragments.len());
    let fragment_count = fragments.len() as u64;
    let start = std::time::Instant::now();
    with_gc_status_write(|st| {
        st.is_running = true;
        st.phase = "Scanning fragments".to_string();
        st.is_manual_run = is_manual_run;
        st.total_fragments = fragment_count;
        st.total_fragments_scanned = 0;
        st.total_live_versions_seen = 0;
        st.total_trash_versions_expired = 0;
        st.total_sessions_deleted = 0;
        st.total_orphan_blob_dirs_deleted = 0;
        st.total_refund_bytes = 0;
        st.duration_seconds = 0;
        st.last_error = None;
        st.start_timestamp = Utc::now().timestamp();
    });

    let retention_duration = Duration::from_secs(CONFIG.garbage_collector.retention_seconds);

    let file_ttl = if is_manual_run {
        Duration::from_secs(1)
    } else {
        Duration::from_secs(CONFIG.garbage_collector.file_ttl_seconds)
    };

    for fragment in fragments {
        if should_stop_gc() {
            break;
        }
        with_gc_status_write(|st| {
            st.current_fragment = fragment.clone();
            st.phase = format!("Processing fragment {}", fragment);
        });

        let stats = process_fragment_gc(
            &fragment,
            &blob_roots,
            retention_duration,
            file_ttl,
            is_manual_run,
            &state,
        )
        .await;

        with_gc_status_write(|st| {
            st.total_fragments_scanned += 1;
            st.total_live_versions_seen += stats.live_versions_seen;
            st.total_trash_versions_expired += stats.trash_versions_expired;
            st.total_sessions_deleted += stats.sessions_deleted;
            st.total_orphan_blob_dirs_deleted += stats.orphan_blob_dirs_deleted;
            st.total_refund_bytes += stats.refund_bytes;
            st.duration_seconds = start.elapsed().as_secs();
        });
    }

    if should_stop_gc() {
        info!("GC: Scan stopped by user request.");
        with_gc_status_write(|st| {
            st.phase = "Stopped by User".to_string();
            st.duration_seconds = start.elapsed().as_secs();
        });
        return Ok(());
    }

    info!("GC: Scan completed.");
    with_gc_status_write(|st| {
        st.phase = "Finalizing".to_string();
        st.duration_seconds = start.elapsed().as_secs();
    });
    Ok(())
}

fn collect_gc_fragments(index_roots: &[PathBuf]) -> HashSet<String> {
    let mut fragments = HashSet::new();
    for root in index_roots {
        if let Ok(entries) = std::fs::read_dir(root) {
            for entry in entries.filter_map(|e| e.ok()) {
                if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    continue;
                }
                let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                    continue;
                };
                let metadata_path = entry.path().join(FRAGMENT_METADATA_FILE);
                if metadata_path.is_file() {
                    fragments.insert(name);
                }
            }
        }
    }
    fragments
}

async fn process_fragment_gc(
    fragment: &str,
    blob_roots: &[PathBuf],
    retention: Duration,
    file_ttl: Duration,
    is_manual_run: bool,
    state: &Arc<AppState>,
) -> FragmentGcStats {
    let mut stats = FragmentGcStats::default();
    let mut live_versions = HashSet::<String>::new();
    let mut trash_versions = HashSet::<String>::new();
    let mut expired_versions: HashMap<String, ObjectMetadata> = HashMap::new();

    let now = SystemTime::now();
    let now_unix = Utc::now().timestamp();
    match index::list_fragment_live_objects(fragment).await {
        Ok(objects) => {
            for meta in objects {
                live_versions.insert(meta.version_id);
            }
        }
        Err(err) => {
            warn!(
                "GC: failed to load live heads for fragment {} from indexdb: {}",
                fragment, err
            );
            return stats;
        }
    }

    match index::list_fragment_trash_catalog(fragment).await {
        Ok(trash_entries) => {
            for entry in trash_entries {
                if should_stop_gc() {
                    return stats;
                }
                let versions =
                    match index::list_trashed_object_versions_metadata(fragment, &entry.object_key)
                        .await
                    {
                        Ok(versions) => versions,
                        Err(err) => {
                            warn!(
                                "GC: failed to load trash history for {}/{}: {}",
                                fragment, entry.object_key, err
                            );
                            continue;
                        }
                    };
                let age = trash_deleted_age(entry.deleted_at, now_unix);
                let should_expire = is_manual_run || age > retention;
                for meta in versions {
                    if should_expire {
                        expired_versions
                            .entry(meta.version_id.clone())
                            .or_insert(meta.clone());
                    } else {
                        trash_versions.insert(meta.version_id);
                    }
                }
            }
        }
        Err(err) => {
            warn!(
                "GC: failed to load trash catalog for fragment {} from indexdb: {}",
                fragment, err
            );
            return stats;
        }
    }
    stats.live_versions_seen = live_versions.len() as u64;

    let expired_trash_refund: u64 = expired_versions
        .values()
        .map(|meta| meta.total_size_bytes)
        .sum();
    stats.refund_bytes += expired_trash_refund;
    stats.trash_versions_expired += expired_versions.len() as u64;

    for (ver, meta) in &expired_versions {
        if should_stop_gc() {
            return stats;
        }
        let _ = index::delete_trashed_object_version(fragment, &meta.object_key, ver).await;
    }
    for root in blob_roots {
        if should_stop_gc() {
            return stats;
        }
        for ver in expired_versions.keys() {
            if should_stop_gc() {
                return stats;
            }
            let live_path = root.join(fragment).join("blob").join(ver);
            let trash_path = get_trash_root(root, fragment).join("blob").join(ver);
            if live_path.exists() {
                let _ = std::fs::remove_dir_all(&live_path);
            }
            if trash_path.exists() {
                let _ = std::fs::remove_dir_all(&trash_path);
            }
        }
    }

    let now_ts = Utc::now().timestamp();
    let ttl_secs = CONFIG.garbage_collector.file_ttl_seconds as i64;
    let mut active_session_versions = HashSet::<String>::new();
    let mut sessions_to_delete = Vec::<(String, u64)>::new();
    if let Ok(active_sessions) = index::list_active_sessions(fragment).await {
        for session in active_sessions {
            let idle_secs = now_ts.saturating_sub(session.last_activity_at.max(0));
            if idle_secs > ttl_secs {
                let amount = session.reservation.map(|r| r.amount).unwrap_or(0);
                sessions_to_delete.push((session.manifest.version_id, amount));
            } else {
                active_session_versions.insert(session.manifest.version_id);
            }
        }
    }

    let total_session_refund: u64 = sessions_to_delete.iter().map(|(_, amount)| *amount).sum();
    stats.refund_bytes += total_session_refund;
    stats.sessions_deleted += sessions_to_delete.len() as u64;

    for (version_id, _) in &sessions_to_delete {
        if should_stop_gc() {
            return stats;
        }
        let _ = index::remove_session(fragment, version_id, "gc-expired").await;
    }

    for root in blob_roots {
        if should_stop_gc() {
            return stats;
        }
        let blob_root = root.join(fragment).join("blob");
        if !blob_root.exists() {
            continue;
        }

        if let Ok(entries) = std::fs::read_dir(&blob_root) {
            for entry in entries.filter_map(|e| e.ok()) {
                if should_stop_gc() {
                    return stats;
                }
                let path = entry.path();
                if !path.is_dir() {
                    continue;
                }

                gc_cleanup_version_dir_unix(&path, now, file_ttl, is_manual_run);

                let ver_id = entry.file_name().to_string_lossy().to_string();

                if !live_versions.contains(&ver_id)
                    && !trash_versions.contains(&ver_id)
                    && !expired_versions.contains_key(&ver_id)
                    && !active_session_versions.contains(&ver_id)
                {
                    let modified = fs::metadata(&path)
                        .and_then(|m| m.modified())
                        .unwrap_or(now);
                    if now.duration_since(modified).unwrap_or(Duration::ZERO) > file_ttl {
                        if std::fs::remove_dir_all(path).is_ok() {
                            stats.orphan_blob_dirs_deleted += 1;
                        }
                    }
                }
            }
        }
    }

    if stats.trash_versions_expired > 0
        || stats.sessions_deleted > 0
        || stats.orphan_blob_dirs_deleted > 0
    {
        state.emit(fragment, StorageEventPayload::StateInvalidated);
    }

    stats
}

#[cfg(test)]
mod tests {
    use super::collect_gc_fragments;
    use crate::core::constants::FRAGMENT_METADATA_FILE;
    use std::fs;
    use ulid::Ulid;

    #[test]
    fn gc_fragment_scan_ignores_filesystem_trash_directories() {
        let base = std::env::temp_dir().join(format!("n1-gc-fragment-scan-{}", Ulid::new()));
        let index_root = base.join("index-root");
        fs::create_dir_all(&index_root).expect("create index root");

        let real_fragment = "real-fragment";
        let real_fragment_dir = index_root.join(real_fragment);
        fs::create_dir_all(&real_fragment_dir).expect("create fragment dir");
        fs::write(real_fragment_dir.join(FRAGMENT_METADATA_FILE), b"fragment")
            .expect("write fragment metadata marker");

        fs::create_dir_all(index_root.join(".Trash-1000")).expect("create desktop trash dir");
        fs::create_dir_all(index_root.join("lost+found")).expect("create unrelated dir");

        let fragments = collect_gc_fragments(&[index_root]);
        assert!(fragments.contains(real_fragment));
        assert!(!fragments.contains(".Trash-1000"));
        assert!(!fragments.contains("lost+found"));

        let _ = fs::remove_dir_all(base);
    }
}
