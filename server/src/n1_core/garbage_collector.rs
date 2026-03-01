use crate::enums::app_error_enum::AppError;
use crate::fs::cleanup_manager::start_cleanup;
use crate::fs::fs_constants::STORAGE_ROOT;
use crate::n1_core::blob_store::{
    RawGcMarker, configured_filesystem_blob_roots, free_raw_address, free_raw_shards_for_metadata,
};
use crate::n1_core::constants::{GC_SUFFIX, METADATA_EXT, QUOTA_FILE, RESERVATION_FILE};
use crate::n1_core::disk_manager::{
    ObjectMetadata, ObjectPathManager, ReservationReceipt, configured_fs_storage_root,
};
use crate::n1_core::quota_manager::update_quota;
use crate::n1_core::sync::sync_single_fragment_quota;
use crate::routes::storage_routes::AppState;
use crate::settings::config::CONFIG;
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
use walkdir::WalkDir;

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

fn gc_status_read_clone() -> GcJobStatus {
    GC_STATUS
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
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
            if st.phase != "Failed" {
                st.phase = "Completed".to_string();
            }
        });
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
    Ok(Json(gc_status_read_clone()))
}

pub async fn run_garbage_collector(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if GC_RUNNING.swap(true, Ordering::SeqCst) {
        return (
            StatusCode::CONFLICT,
            "GC is already running. Check /v4/maintenance/gc/status.",
        );
    }

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

        start_cleanup(&configured_fs_storage_root().unwrap_or_else(|| STORAGE_ROOT.to_string()))
            .await;

        with_gc_status_write(|st| {
            st.is_running = false;
            if st.phase != "Failed" {
                st.phase = "Completed".to_string();
            }
        });
        GC_RUNNING.store(false, Ordering::SeqCst);
    });

    (StatusCode::ACCEPTED, "GC started")
}

async fn refund_quota(fragment: &str, amount: u64, state: &Arc<AppState>) {
    if amount == 0 {
        return;
    }

    if let Err(e) = update_quota(fragment, -(amount as i64)).await {
        warn!("Failed to refund quota for fragment {}: {}", fragment, e);
    } else {
        info!("GC: Refunded {} bytes to fragment {}", amount, fragment);
        state.emit(fragment, StorageEventPayload::StateInvalidated);
    }
}

fn get_trash_root(disk_root: &std::path::Path, fragment_hash: &str) -> PathBuf {
    disk_root.join(fragment_hash).join(".trash")
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

        let modified = md.modified().unwrap_or(now);
        let age = now.duration_since(modified).unwrap_or(Duration::ZERO);
        if !(is_manual || age > gc_ttl) {
            continue;
        }

        if name.ends_with(GC_SUFFIX) {
            gc_files.push((name, p));
            continue;
        }

        if name.contains(".preexisting.") {
            preexisting_files.push(p);
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

        let modified = md.modified().unwrap_or(now);
        let age = now.duration_since(modified).unwrap_or(Duration::ZERO);
        if !(is_manual || age > gc_ttl) {
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
    let blob_roots = configured_filesystem_blob_roots().unwrap_or_default();

    let mut fragments = HashSet::new();
    for root in &index_roots {
        if let Ok(entries) = std::fs::read_dir(root) {
            for entry in entries.filter_map(|e| e.ok()) {
                if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    if let Some(name) = entry.file_name().to_str() {
                        fragments.insert(name.to_string());
                    }
                }
            }
        }
    }

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
        with_gc_status_write(|st| {
            st.current_fragment = fragment.clone();
            st.phase = format!("Processing fragment {}", fragment);
        });

        let stats = process_fragment_gc(
            &fragment,
            &index_roots,
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

    info!("GC: Scan completed.");
    with_gc_status_write(|st| {
        st.phase = "Finalizing".to_string();
        st.duration_seconds = start.elapsed().as_secs();
    });
    Ok(())
}

async fn process_fragment_gc(
    fragment: &str,
    index_roots: &[PathBuf],
    blob_roots: &[PathBuf],
    retention: Duration,
    file_ttl: Duration,
    is_manual_run: bool,
    state: &Arc<AppState>,
) -> FragmentGcStats {
    let mut stats = FragmentGcStats::default();
    let mut live_versions = HashSet::new();
    let mut trash_versions = HashSet::new();
    let mut expired_trash_refund = 0u64;
    let mut expired_versions = HashSet::new();
    let mut expired_raw_metas: Vec<ObjectMetadata> = Vec::new();
    let mut eligible_raw_gc_markers: HashMap<
        String,
        (crate::n1_core::disk_manager::RawBlobAddress, Vec<PathBuf>),
    > = HashMap::new();

    for root in index_roots {
        let index_dir = root.join(fragment).join("index");
        if !index_dir.exists() {
            continue;
        }

        for entry in WalkDir::new(&index_dir).into_iter().filter_map(|e| e.ok()) {
            if entry.file_type().is_file() {
                let fname = entry.file_name().to_string_lossy();
                if fname.ends_with(METADATA_EXT) && fname != QUOTA_FILE && fname != RESERVATION_FILE
                {
                    if let Some((_, rest)) = fname.split_once('#') {
                        if let Some(ver) = rest.strip_suffix(METADATA_EXT) {
                            live_versions.insert(ver.to_string());
                        }
                    }
                }
            }
        }
        if !live_versions.is_empty() {
            break;
        }
    }
    stats.live_versions_seen = live_versions.len() as u64;

    let now = SystemTime::now();

    for root in index_roots {
        let raw_gc_dir = root.join(fragment).join("index").join(".raw_gc");
        if !raw_gc_dir.exists() {
            continue;
        }

        for entry in WalkDir::new(&raw_gc_dir).into_iter().filter_map(|e| e.ok()) {
            if !entry.file_type().is_file() {
                continue;
            }

            let modified = entry
                .metadata()
                .ok()
                .and_then(|m| m.modified().ok())
                .unwrap_or(now);
            let age = now.duration_since(modified).unwrap_or(Duration::ZERO);
            if !(is_manual_run || age > file_ttl) {
                continue;
            }

            let Ok(bytes) = std::fs::read(entry.path()) else {
                continue;
            };
            let Ok(marker) = rmp_serde::from_slice::<RawGcMarker>(&bytes) else {
                continue;
            };

            let key = format!(
                "{}:{}:{}:{}",
                marker.address.device_id,
                marker.address.allocation_id,
                marker.address.offset_bytes,
                marker.address.size_bytes
            );
            let entry_path = entry.path().to_path_buf();
            eligible_raw_gc_markers
                .entry(key)
                .and_modify(|(_, paths)| paths.push(entry_path.clone()))
                .or_insert((marker.address, vec![entry_path]));
        }
    }

    for root in index_roots {
        let trash_index = get_trash_root(root, fragment).join("index");
        if !trash_index.exists() {
            continue;
        }

        for entry in WalkDir::new(&trash_index)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_file()
                && entry.file_name().to_string_lossy().ends_with(METADATA_EXT)
            {
                let fname = entry.file_name().to_string_lossy().to_string();

                let modified = entry
                    .metadata()
                    .ok()
                    .and_then(|m| m.modified().ok())
                    .unwrap_or(now);
                let age = now.duration_since(modified).unwrap_or(Duration::ZERO);

                let should_expire = age > retention || is_manual_run;

                let mut ver_id = String::new();
                if let Some((_, rest)) = fname.split_once('#') {
                    if let Some(ver) = rest.strip_suffix(METADATA_EXT) {
                        ver_id = ver.to_string();
                    }
                }

                if ver_id.is_empty() {
                    continue;
                }

                if should_expire {
                    if let Ok(data) = std::fs::read(entry.path()) {
                        if let Ok(meta) = rmp_serde::from_slice::<ObjectMetadata>(&data) {
                            if !expired_versions.contains(&ver_id) {
                                expired_trash_refund += meta.total_size_bytes;
                                expired_versions.insert(ver_id.clone());
                            }
                            if meta.parts.iter().any(|part| {
                                part.shards
                                    .iter()
                                    .any(|shard| shard.raw_blob_address.is_some())
                            }) {
                                expired_raw_metas.push(meta);
                            }
                        }
                    }
                } else {
                    trash_versions.insert(ver_id);
                }
            }
        }
        if !expired_versions.is_empty() || !trash_versions.is_empty() {
            break;
        }
    }

    refund_quota(fragment, expired_trash_refund, state).await;
    stats.refund_bytes += expired_trash_refund;
    stats.trash_versions_expired += expired_versions.len() as u64;

    for meta in &expired_raw_metas {
        let _ = free_raw_shards_for_metadata(meta).await;
    }

    for (_, (raw, marker_paths)) in eligible_raw_gc_markers {
        match free_raw_address(&raw).await {
            Ok(_) => {
                for marker_path in marker_paths {
                    let _ = std::fs::remove_file(&marker_path);
                }
            }
            Err(e) => {
                warn!(
                    "GC: Failed to free redistributed raw extent {}:{}:{}:{}: {}",
                    raw.device_id, raw.allocation_id, raw.offset_bytes, raw.size_bytes, e
                );
            }
        }
    }

    if !expired_versions.is_empty() {
        let tasks = index_roots.iter().chain(blob_roots.iter()).map(|root| {
            let frag = fragment.to_string();
            let expired = expired_versions.clone();
            let root_path = root.clone();

            tokio::task::spawn_blocking(move || {
                let trash_idx = get_trash_root(&root_path, &frag).join("index");
                if trash_idx.exists() {
                    for entry in WalkDir::new(&trash_idx).into_iter().filter_map(|e| e.ok()) {
                        let fname = entry.file_name().to_string_lossy();
                        if fname.ends_with(METADATA_EXT) {
                            if let Some((_, rest)) = fname.split_once('#') {
                                if let Some(ver) = rest.strip_suffix(METADATA_EXT) {
                                    if expired.contains(ver) {
                                        let _ = std::fs::remove_file(entry.path());
                                    }
                                }
                            }
                        }
                    }
                }

                let trash_blob_root = get_trash_root(&root_path, &frag).join("blob");
                for ver in &expired {
                    let p = trash_blob_root.join(ver);
                    if p.exists() {
                        let _ = std::fs::remove_dir_all(p);
                    }
                }
            })
        });
        futures_util::future::join_all(tasks).await;
    }

    let mut sessions_to_delete = HashSet::new();
    let mut session_amounts: HashMap<String, u64> = HashMap::new();

    for root in index_roots {
        let session_root = root.join(fragment).join("index").join("sessions");
        if !session_root.exists() {
            continue;
        }

        if let Ok(entries) = std::fs::read_dir(&session_root) {
            for entry in entries.filter_map(|e| e.ok()) {
                let path = entry.path();
                if !path.is_dir() {
                    continue;
                }

                let session_id = entry.file_name().to_string_lossy().to_string();

                if sessions_to_delete.contains(&session_id) {
                    if !session_amounts.contains_key(&session_id) {
                        let res_file = path.join(RESERVATION_FILE);
                        if let Ok(data) = std::fs::read(&res_file) {
                            if let Ok(receipt) = rmp_serde::from_slice::<ReservationReceipt>(&data)
                            {
                                session_amounts.insert(session_id.clone(), receipt.amount);
                            }
                        }
                    }
                    continue;
                }

                let modified = fs::metadata(&path)
                    .and_then(|m| m.modified())
                    .unwrap_or(now);

                if now.duration_since(modified).unwrap_or(Duration::ZERO) > file_ttl {
                    sessions_to_delete.insert(session_id.clone());

                    let res_file = path.join(RESERVATION_FILE);
                    if res_file.exists() {
                        if let Ok(data) = std::fs::read(&res_file) {
                            if let Ok(receipt) = rmp_serde::from_slice::<ReservationReceipt>(&data)
                            {
                                session_amounts.insert(session_id, receipt.amount);
                            }
                        }
                    }
                }
            }
        }
    }

    for sess in &sessions_to_delete {
        if !session_amounts.contains_key(sess) {
            warn!(
                "GC: Session {} deleted without refund (missing reservation receipt). Quota may drift. Run 'sync_fragment_metadata' to fix.",
                sess
            );
        }
    }

    let total_session_refund: u64 = session_amounts.values().sum();
    refund_quota(fragment, total_session_refund, state).await;
    stats.refund_bytes += total_session_refund;
    stats.sessions_deleted += sessions_to_delete.len() as u64;

    for root in index_roots {
        for sess_id in &sessions_to_delete {
            let p = root
                .join(fragment)
                .join("index")
                .join("sessions")
                .join(sess_id);
            if p.exists() {
                let _ = std::fs::remove_dir_all(p);
            }
        }
    }

    for root in blob_roots {
        let blob_root = root.join(fragment).join("blob");
        if !blob_root.exists() {
            continue;
        }

        if let Ok(entries) = std::fs::read_dir(&blob_root) {
            for entry in entries.filter_map(|e| e.ok()) {
                let path = entry.path();
                if !path.is_dir() {
                    continue;
                }

                gc_cleanup_version_dir_unix(&path, now, file_ttl, is_manual_run);

                let ver_id = entry.file_name().to_string_lossy().to_string();

                if !live_versions.contains(&ver_id)
                    && !trash_versions.contains(&ver_id)
                    && !expired_versions.contains(&ver_id)
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

    if let Err(e) = sync_single_fragment_quota(fragment.to_string()).await {
        warn!(
            "GC: Fragment quota reconciliation failed for {}: {}",
            fragment, e
        );
    }

    stats
}
