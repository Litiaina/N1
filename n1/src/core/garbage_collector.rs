use crate::core::constants::{FRAGMENT_METADATA_FILE, GC_SUFFIX, N1_BLOB_DIR};
use crate::core::disk_manager::{FS_STORAGE_ROOT, ObjectMetadata, ObjectPathManager};
use crate::core::index;
use crate::core::operation_gate::{MaintenanceAccessMode, acquire_maintenance_mode};
use crate::enums::app_error_enum::AppError;
use crate::fs::cleanup_manager::start_cleanup;
use crate::fs::fs_constants::STORAGE_ROOT;
use crate::namespace::runtime::global_namespace_runtime;
use crate::routes::storage_routes::AppState;
use crate::settings::config::CONFIG;
use crate::structs::auth_structs::Claims;
use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use chrono::Utc;
use serde::Serialize;
use std::collections::{BTreeSet, HashMap, HashSet};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GcExecutionMode {
    Availability,
    Strict,
    Scrub,
}

impl GcExecutionMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Availability => "availability",
            Self::Strict => "strict",
            Self::Scrub => "scrub",
        }
    }

    fn maintenance_access_mode(self) -> MaintenanceAccessMode {
        match self {
            Self::Availability => MaintenanceAccessMode::Availability,
            Self::Strict => MaintenanceAccessMode::ReadOnly,
            Self::Scrub => MaintenanceAccessMode::Quiesced,
        }
    }

    fn file_ttl(self, is_manual_run: bool) -> Duration {
        if !is_manual_run {
            return Duration::from_secs(CONFIG.garbage_collector.file_ttl_seconds);
        }
        match self {
            Self::Availability => Duration::from_secs(1),
            Self::Strict => Duration::from_secs(1),
            Self::Scrub => Duration::ZERO,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct GcRunOptions {
    execution_mode: GcExecutionMode,
    fast_mode: bool,
}

impl Default for GcRunOptions {
    fn default() -> Self {
        Self {
            execution_mode: GcExecutionMode::Availability,
            fast_mode: false,
        }
    }
}

impl GcRunOptions {
    fn file_ttl(self, is_manual_run: bool) -> Duration {
        if !self.fast_mode {
            return self.execution_mode.file_ttl(is_manual_run);
        }
        if !is_manual_run {
            return Duration::from_secs(CONFIG.garbage_collector.file_ttl_seconds);
        }
        match self.execution_mode {
            GcExecutionMode::Availability => Duration::from_secs(5),
            GcExecutionMode::Strict => Duration::from_secs(1),
            GcExecutionMode::Scrub => Duration::ZERO,
        }
    }
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

async fn run_gc_scan_offthread(
    state: Arc<AppState>,
    is_manual_run: bool,
    target_fragment: Option<String>,
    options: GcRunOptions,
) -> Result<(), AppError> {
    let handle = Handle::current();
    let join = tokio::task::spawn_blocking(move || {
        handle.block_on(perform_gc_scan(
            state,
            is_manual_run,
            target_fragment,
            options,
        ))
    })
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
        sleep(cleanup_interval).await;

        if GC_RUNNING.swap(true, Ordering::SeqCst) {
            continue;
        }
        GC_STOP_SIGNAL.store(false, Ordering::Relaxed);

        if let Err(err) =
            run_gc_scan_offthread(state.clone(), false, None, GcRunOptions::default()).await
        {
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

fn parse_gc_target_fragment(params: &HashMap<String, String>) -> Option<String> {
    params
        .get("fragment")
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn parse_bool_param(params: &HashMap<String, String>, key: &str, default: bool) -> bool {
    params
        .get(key)
        .map(|v| v.trim().to_ascii_lowercase())
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes" | "on" | "fast"))
        .unwrap_or(default)
}

fn parse_gc_execution_mode(params: &HashMap<String, String>) -> GcExecutionMode {
    let mode = params
        .get("mode")
        .or_else(|| params.get("execution_mode"))
        .or_else(|| params.get("access_mode"))
        .map(|value| value.trim().to_ascii_lowercase());
    match mode.as_deref() {
        Some("strict") | Some("readonly") | Some("read_only") => GcExecutionMode::Strict,
        Some("scrub") | Some("quiesced") | Some("offline") => GcExecutionMode::Scrub,
        _ => GcExecutionMode::Availability,
    }
}

pub async fn run_garbage_collector(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let target_fragment = parse_gc_target_fragment(&params);
    let execution_mode = parse_gc_execution_mode(&params);
    let fast_mode = parse_bool_param(
        &params,
        "fast_mode",
        execution_mode != GcExecutionMode::Availability,
    );
    let options = GcRunOptions {
        execution_mode,
        fast_mode,
    };
    if let Some(fragment) = target_fragment.as_ref() {
        let path_manager = ObjectPathManager::new();
        if !collect_gc_fragments(&path_manager.get_all_index_roots()).contains(fragment) {
            return (
                StatusCode::NOT_FOUND,
                format!("Fragment {} was not found.", fragment),
            );
        }
    }

    if GC_RUNNING.swap(true, Ordering::SeqCst) {
        return (
            StatusCode::CONFLICT,
            "GC is already running. Check /n1/maintenance/gc/status.".to_string(),
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
                    current_fragment: target_fragment.clone().unwrap_or_default(),
                    start_timestamp: Utc::now().timestamp(),
                    ..Default::default()
                };
            });
        }

        if let Err(err) = run_gc_scan_offthread(state.clone(), true, target_fragment, options).await
        {
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

    (StatusCode::ACCEPTED, "GC started".to_string())
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

#[inline]
fn orphan_blob_age_expired(age: Duration, ttl: Duration) -> bool {
    ttl.is_zero() || age >= ttl
}

async fn perform_gc_scan(
    state: Arc<AppState>,
    is_manual_run: bool,
    target_fragment: Option<String>,
    options: GcRunOptions,
) -> Result<(), AppError> {
    let _maintenance_gate =
        acquire_maintenance_mode(options.execution_mode.maintenance_access_mode());
    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();
    let blob_roots = path_manager.get_blob_roots();

    let mut fragments = collect_gc_fragments(&index_roots)
        .into_iter()
        .collect::<Vec<_>>();
    fragments.sort();
    if let Some(target) = target_fragment {
        if !fragments.iter().any(|fragment| fragment == &target) {
            return Err(AppError::NotFound);
        }
        fragments.retain(|fragment| fragment == &target);
    }

    info!(
        "GC: Starting scan for {} fragments (mode={}, fast_mode={})...",
        fragments.len(),
        options.execution_mode.as_str(),
        options.fast_mode
    );
    let fragment_count = fragments.len() as u64;
    let start = std::time::Instant::now();
    with_gc_status_write(|st| {
        st.is_running = true;
        st.phase = format!(
            "Scanning fragments (mode={}, fast_mode={})",
            options.execution_mode.as_str(),
            options.fast_mode
        );
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

    let file_ttl = options.file_ttl(is_manual_run);

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
            options,
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
    _options: GcRunOptions,
    _state: &Arc<AppState>,
) -> FragmentGcStats {
    let mut stats = FragmentGcStats::default();
    let mut live_versions = HashSet::<String>::new();
    let mut trash_versions = HashSet::<String>::new();
    let mut expired_versions: HashMap<String, ObjectMetadata> = HashMap::new();
    let mut purged_namespace_version_ids = HashSet::<String>::new();

    let now = SystemTime::now();
    let now_unix = Utc::now().timestamp();
    let physical_live_objects = match index::list_fragment_live_objects(fragment).await {
        Ok(objects) => objects,
        Err(err) => {
            warn!(
                "GC: failed to load live heads for fragment {} from N1 index: {}",
                fragment, err
            );
            return stats;
        }
    };

    let namespace_snapshot = match global_namespace_runtime()
        .reachability_snapshot_if_present(fragment)
        .await
    {
        Ok(snapshot) => snapshot,
        Err(err) => {
            warn!(
                "GC: failed to load namespace reachability for fragment {}: {}",
                fragment, err
            );
            None
        }
    };

    let mut protected_live_object_ids = BTreeSet::<String>::new();
    if let Some(snapshot) = &namespace_snapshot {
        protected_live_object_ids.extend(snapshot.referenced_object_ids.iter().cloned());
        live_versions.extend(snapshot.referenced_version_ids.iter().cloned());
    } else {
        for meta in &physical_live_objects {
            protected_live_object_ids.insert(meta.object_key.clone());
            match index::list_live_object_versions_metadata(fragment, &meta.object_key).await {
                Ok(version_metas) => {
                    if version_metas.is_empty() {
                        live_versions.insert(meta.version_id.clone());
                    } else {
                        live_versions
                            .extend(version_metas.into_iter().map(|version| version.version_id));
                    }
                }
                Err(err) => {
                    warn!(
                        "GC: failed to load live version history for protected object {}/{}: {}; preserving head and skipping destructive namespace purge for this fragment",
                        fragment, meta.object_key, err
                    );
                    live_versions.insert(meta.version_id.clone());
                    stats.live_versions_seen = live_versions.len() as u64;
                    return stats;
                }
            }
        }
    }

    let mut purged_object_ids = match global_namespace_runtime()
        .purged_object_ids_for_gc(fragment)
        .await
    {
        Ok(object_ids) => object_ids,
        Err(err) => {
            warn!(
                "GC: failed to load purged namespace object ids for fragment {}: {}",
                fragment, err
            );
            BTreeSet::new()
        }
    };
    if namespace_snapshot.is_none() && !purged_object_ids.is_empty() {
        warn!(
            "GC: preserving {} purged namespace object(s) for fragment {} because namespace reachability was unavailable",
            purged_object_ids.len(),
            fragment
        );
        purged_object_ids.clear();
    }

    let mut namespace_absent_gc_candidates = BTreeSet::<String>::new();
    if namespace_snapshot.is_none() {
        namespace_absent_gc_candidates.extend(purged_object_ids.iter().cloned());
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
                "GC: failed to load trash catalog for fragment {} from N1 index: {}",
                fragment, err
            );
            return stats;
        }
    }

    for object_id in purged_object_ids {
        if should_stop_gc() {
            return stats;
        }
        if namespace_snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.referenced_object_ids.contains(&object_id))
        {
            warn!(
                "GC: preserving purged namespace object {}/{} because it is still referenced by live/trash namespace rows",
                fragment, object_id
            );
            continue;
        }
        if !protected_live_object_ids.contains(&object_id) {
            continue;
        }

        let mut version_metas = match index::list_live_object_versions_metadata(
            fragment, &object_id,
        )
        .await
        {
            Ok(versions) => versions,
            Err(err) => {
                warn!(
                    "GC: failed to load live version history for purged namespace object {}/{}: {}",
                    fragment, object_id, err
                );
                continue;
            }
        };
        match index::list_trashed_object_versions_metadata(fragment, &object_id).await {
            Ok(versions) => version_metas.extend(versions),
            Err(err) => warn!(
                "GC: failed to load trash version history for purged namespace object {}/{}: {}",
                fragment, object_id, err
            ),
        }
        let refund_bytes = version_metas
            .iter()
            .map(|version| version.total_size_bytes)
            .sum::<u64>();
        match index::delete_object_fast(fragment, &object_id).await {
            Ok(Some(version_ids)) => {
                purged_namespace_version_ids.extend(version_ids);
                stats.refund_bytes = stats.refund_bytes.saturating_add(refund_bytes);
            }
            Ok(None) => {}
            Err(err) => {
                warn!(
                    "GC: failed to delete purged namespace object {}/{}: {}",
                    fragment, object_id, err
                );
            }
        }
    }

    for object_id in namespace_absent_gc_candidates {
        if should_stop_gc() {
            return stats;
        }
        if namespace_snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.referenced_object_ids.contains(&object_id))
        {
            continue;
        }

        let mut version_metas =
            match index::list_live_object_versions_metadata(fragment, &object_id).await {
                Ok(versions) => versions,
                Err(err) => {
                    warn!(
                        "GC: failed to load unbound live version history for {}/{}: {}",
                        fragment, object_id, err
                    );
                    continue;
                }
            };
        match index::list_trashed_object_versions_metadata(fragment, &object_id).await {
            Ok(versions) => version_metas.extend(versions),
            Err(err) => warn!(
                "GC: failed to load unbound trash version history for {}/{}: {}",
                fragment, object_id, err
            ),
        }
        let refund_bytes = version_metas
            .iter()
            .map(|version| version.total_size_bytes)
            .sum::<u64>();
        for version in &version_metas {
            live_versions.remove(&version.version_id);
            trash_versions.remove(&version.version_id);
        }

        match index::delete_object_fast(fragment, &object_id).await {
            Ok(Some(version_ids)) => {
                purged_namespace_version_ids.extend(version_ids);
                stats.refund_bytes = stats.refund_bytes.saturating_add(refund_bytes);
            }
            Ok(None) => {}
            Err(err) => {
                warn!(
                    "GC: failed to delete unbound completed object {}/{}: {}",
                    fragment, object_id, err
                );
            }
        }
    }

    expired_versions.retain(|version_id, meta| {
        if live_versions.contains(version_id) {
            warn!(
                "GC: preserving expired trash version {}/{}@{} because it is still protected as live",
                fragment, meta.object_key, version_id
            );
            false
        } else {
            true
        }
    });
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
            let live_path = root.join(fragment).join(N1_BLOB_DIR).join(ver);
            let trash_path = get_trash_root(root, fragment).join(N1_BLOB_DIR).join(ver);
            if live_path.exists() {
                let _ = std::fs::remove_dir_all(&live_path);
            }
            if trash_path.exists() {
                let _ = std::fs::remove_dir_all(&trash_path);
            }
        }
        for ver in &purged_namespace_version_ids {
            if should_stop_gc() {
                return stats;
            }
            let live_path = root.join(fragment).join(N1_BLOB_DIR).join(ver);
            let trash_path = get_trash_root(root, fragment).join(N1_BLOB_DIR).join(ver);
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
        let blob_root = root.join(fragment).join(N1_BLOB_DIR);
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
                    let orphan_age = now.duration_since(modified).unwrap_or(Duration::ZERO);
                    if orphan_blob_age_expired(orphan_age, file_ttl) {
                        if std::fs::remove_dir_all(path).is_ok() {
                            stats.orphan_blob_dirs_deleted += 1;
                        }
                    }
                }
            }
        }
    }

    stats
}

#[cfg(test)]
mod tests {
    use super::{GcExecutionMode, GcRunOptions, collect_gc_fragments, parse_gc_execution_mode};
    use crate::core::constants::FRAGMENT_METADATA_FILE;
    use std::fs;
    use std::time::Duration;
    use ulid::Ulid;

    #[test]
    fn gc_execution_mode_parsing_and_fast_options_are_aggressive_only_when_requested() {
        let mut params = std::collections::HashMap::new();
        assert_eq!(
            parse_gc_execution_mode(&params),
            GcExecutionMode::Availability
        );

        params.insert("mode".to_string(), "strict".to_string());
        assert_eq!(parse_gc_execution_mode(&params), GcExecutionMode::Strict);

        params.insert("mode".to_string(), "quiesced".to_string());
        assert_eq!(parse_gc_execution_mode(&params), GcExecutionMode::Scrub);

        let availability = GcRunOptions {
            execution_mode: GcExecutionMode::Availability,
            fast_mode: false,
        };
        let strict_fast = GcRunOptions {
            execution_mode: GcExecutionMode::Strict,
            fast_mode: true,
        };
        let scrub_fast = GcRunOptions {
            execution_mode: GcExecutionMode::Scrub,
            fast_mode: true,
        };

        assert_eq!(availability.file_ttl(true), Duration::from_secs(1));
        assert_eq!(strict_fast.file_ttl(true), Duration::from_secs(1));
        assert_eq!(scrub_fast.file_ttl(true), Duration::ZERO);
    }

    #[test]
    fn scrub_zero_ttl_reclaims_fresh_orphan_blob_dirs() {
        assert!(super::orphan_blob_age_expired(
            Duration::ZERO,
            Duration::ZERO
        ));
        assert!(!super::orphan_blob_age_expired(
            Duration::from_millis(999),
            Duration::from_secs(1)
        ));
        assert!(super::orphan_blob_age_expired(
            Duration::from_secs(1),
            Duration::from_secs(1)
        ));
    }

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
