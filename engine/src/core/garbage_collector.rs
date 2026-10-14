use crate::core::constants::{FRAGMENT_METADATA_FILE, GC_SUFFIX};
use crate::core::disk_manager::ObjectPathManager;
use crate::core::fragment_identity::{
    fragment_root_on, list_fragment_aliases, stable_fragment_id_for_root,
};
use crate::core::index;
use crate::core::index_store;
use crate::core::io_scheduler::{DiskIoClass, IoProgress, acquire_disk_io};
use crate::core::operation_gate::safety_read_only_reason;
use crate::core::operation_gate::{MaintenanceAccessMode, acquire_maintenance_mode};
use crate::enums::app_error_enum::AppError;
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

mod reclaim;
pub(crate) use reclaim::reclaim_object_versions;

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

    let _ = state;
    loop {
        if safety_read_only_reason().is_some() {
            sleep(cleanup_interval).await;
            continue;
        }
        if GC_RUNNING.swap(true, Ordering::SeqCst) {
            sleep(cleanup_interval).await;
            continue;
        }
        GC_STOP_SIGNAL.store(false, Ordering::Relaxed);

        let path_manager = ObjectPathManager::new();
        let index_roots = path_manager.get_all_index_roots();
        let mut fragments = collect_gc_fragments(&index_roots)
            .into_iter()
            .collect::<Vec<_>>();
        fragments.sort();
        let started = std::time::Instant::now();
        with_gc_status_write(|st| {
            *st = GcJobStatus {
                is_running: true,
                phase: "Scanning trash lifecycle namespace".to_string(),
                is_manual_run: false,
                total_fragments: fragments.len() as u64,
                start_timestamp: Utc::now().timestamp(),
                ..Default::default()
            };
        });

        for fragment in fragments {
            if should_stop_gc() {
                break;
            }
            with_gc_status_write(|st| {
                st.current_fragment = fragment.clone();
                st.phase = format!("Scanning .trash for fragment {fragment}");
            });
            match index_store::fragment_meta(&fragment) {
                Ok(Some(_)) => {}
                Ok(None) => {
                    with_gc_status_write(|st| {
                        st.total_errors = st.total_errors.saturating_add(1);
                        st.last_error =
                            Some(format!("fragment {fragment} has no N1 3 engine metadata"));
                    });
                    continue;
                }
                Err(err) => {
                    with_gc_status_write(|st| {
                        st.total_errors = st.total_errors.saturating_add(1);
                        st.last_error = Some(format!(
                            "fragment {fragment} failed the N1 3 engine-format gate: {err}"
                        ));
                    });
                    continue;
                }
            }
            match global_namespace_runtime()
                .sweep_expired_trash(
                    &fragment,
                    CONFIG.garbage_collector.trash_retention_seconds,
                    Utc::now().timestamp(),
                )
                .await
            {
                Ok(summary) => with_gc_status_write(|st| {
                    st.total_fragments_scanned = st.total_fragments_scanned.saturating_add(1);
                    st.total_trash_versions_expired = st
                        .total_trash_versions_expired
                        .saturating_add(summary.expired_versions);
                    st.duration_seconds = started.elapsed().as_secs();
                }),
                Err(err) => {
                    error!("Trash lifecycle scan failed for {}: {}", fragment, err);
                    with_gc_status_write(|st| {
                        st.total_errors = st.total_errors.saturating_add(1);
                        st.last_error = Some(err.to_string());
                    });
                }
            }
        }

        with_gc_status_write(|st| {
            st.is_running = false;
            st.phase = if should_stop_gc() {
                "Stopped by User".to_string()
            } else if st.total_errors > 0 {
                "Completed with errors".to_string()
            } else {
                "Trash lifecycle completed".to_string()
            };
            st.duration_seconds = started.elapsed().as_secs();
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
    claims: Claims,
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    if let Some(reason) = safety_read_only_reason() {
        return (StatusCode::SERVICE_UNAVAILABLE, reason);
    }
    let execution_mode = parse_gc_execution_mode(&params);
    let target_fragment = match crate::middleware::maintenance_auth::target(
        &claims,
        &params,
        execution_mode != GcExecutionMode::Availability,
    )
    .await
    {
        Ok(target) => target,
        Err(err) => return (err.status_code(), err.to_string()),
    };
    let fast_mode = parse_bool_param(
        &params,
        "fast_mode",
        execution_mode != GcExecutionMode::Availability,
    );
    let options = GcRunOptions {
        execution_mode,
        fast_mode,
    };
    if GC_RUNNING.swap(true, Ordering::SeqCst) {
        return (
            StatusCode::CONFLICT,
            "GC is already running. Check /noa/v1/maintenance/gc/status.".to_string(),
        );
    }
    GC_STOP_SIGNAL.store(false, Ordering::Relaxed);
    // Publish admission before returning 202: immediate status polling must
    // never mistake the preceding job's completion for this job's completion.
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
    tokio::spawn(async move {
        if let Err(err) = run_gc_scan_offthread(state.clone(), true, target_fragment, options).await
        {
            error!("Manual GC Scan failed: {}", err);
            with_gc_status_write(|st| {
                st.total_errors += 1;
                st.last_error = Some(err.to_string());
                st.phase = "Failed".to_string();
            });
        }

        with_gc_status_write(|st| {
            st.is_running = false;
            if st.phase != "Failed" && st.phase != "Stopped by User" {
                st.phase = if st.total_errors > 0 {
                    "Completed with errors"
                } else {
                    "Completed"
                }
                .to_string();
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
    if claims.require_operator().is_err() {
        return (
            StatusCode::FORBIDDEN,
            "Master-key operator authentication required",
        );
    }
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

#[cfg(unix)]
fn gc_cleanup_version_dir_unix(
    version_dir: &Path,
    now: SystemTime,
    gc_ttl: Duration,
    is_manual: bool,
) {
    let _snapshot_boundary = crate::core::backup::barrier::mutation();
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

fn orphan_version_reclaimable(
    version_id: &str,
    is_committed: bool,
    is_active: bool,
    age: Duration,
    ttl: Duration,
) -> bool {
    !is_committed
        && !is_active
        && ulid::Ulid::from_string(version_id).is_ok()
        && orphan_blob_age_expired(age, ttl)
}

fn prune_orphan_object_dir(object_dir: &Path, now: SystemTime, ttl: Duration) -> bool {
    let _snapshot_boundary = crate::core::backup::barrier::mutation();
    let Ok(metadata) = fs::symlink_metadata(object_dir) else {
        return false;
    };
    if !metadata.file_type().is_dir() {
        return false;
    }
    let Ok(modified) = metadata.modified() else {
        return false;
    };
    let age = now.duration_since(modified).unwrap_or(Duration::ZERO);
    // A parent's mtime does not describe activity in its descendants. Only
    // prune an EMPTY parent: young versions, unknown files, failed deletions,
    // and children created after the scan must remain intact. The kernel's
    // empty-directory check also closes the check-then-remove race here.
    orphan_blob_age_expired(age, ttl) && fs::remove_dir(object_dir).is_ok()
}

fn record_gc_preservation_error(fragment: &str, context: &str, error: impl std::fmt::Display) {
    let message = format!("GC preserving fragment {fragment}: {context}: {error}");
    warn!("{message}");
    with_gc_status_write(|status| {
        status.total_errors = status.total_errors.saturating_add(1);
        status.last_error = Some(message);
    });
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
    let scheduler_fragment = if fragments.len() == 1 {
        fragments.first().map(String::as_str)
    } else {
        None
    };
    let io_guard = acquire_disk_io(
        DiskIoClass::BackgroundGc,
        scheduler_fragment,
        "garbage collection",
    )
    .await?;
    let io_progress = io_guard.progress_handle();
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
        st.total_errors = 0;
        st.duration_seconds = 0;
        st.last_error = None;
        st.start_timestamp = Utc::now().timestamp();
    });

    let retention_duration = Duration::from_secs(CONFIG.garbage_collector.trash_retention_seconds);

    let file_ttl = options.file_ttl(is_manual_run);
    let export_gc_protection = match crate::core::export::gc_protected_fragments().await {
        Ok(snapshot) => snapshot,
        Err(err) => {
            warn!(
                "GC: preserving immutable data because active export protection could not be proven: {}",
                err
            );
            with_gc_status_write(|st| {
                st.phase = "Preserved: export protection unavailable".to_string();
                st.last_error = Some(err.to_string());
                st.total_errors = st.total_errors.saturating_add(1);
                st.duration_seconds = start.elapsed().as_secs();
            });
            io_guard.complete();
            return Ok(());
        }
    };

    for fragment in fragments {
        if should_stop_gc() {
            break;
        }
        io_guard.heartbeat();
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
            &io_progress,
            &export_gc_protection,
        )
        .await;
        io_guard.record_progress(stats.refund_bytes);

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
        io_guard.complete();
        return Ok(());
    }

    info!("GC: Scan completed.");
    with_gc_status_write(|st| {
        st.phase = "Finalizing".to_string();
        st.duration_seconds = start.elapsed().as_secs();
    });
    io_guard.complete();
    Ok(())
}

fn collect_gc_fragments(index_roots: &[PathBuf]) -> HashSet<String> {
    let mut fragments = HashSet::new();
    for root in index_roots {
        for name in list_fragment_aliases(root) {
            let metadata_path = fragment_root_on(root, &name).join(FRAGMENT_METADATA_FILE);
            if metadata_path.is_file() {
                fragments.insert(stable_fragment_id_for_root(root, &name).unwrap_or(name));
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
    io_progress: &IoProgress,
    export_gc_protection: &HashSet<String>,
) -> FragmentGcStats {
    let mut stats = FragmentGcStats::default();

    match index_store::fragment_meta(fragment) {
        Ok(Some(_)) => {}
        Ok(None) => {
            record_gc_preservation_error(
                fragment,
                "engine metadata unavailable",
                "missing N1 3 metadata",
            );
            return stats;
        }
        Err(err) => {
            record_gc_preservation_error(fragment, "engine-format gate failed", err);
            return stats;
        }
    }

    if export_gc_protection.contains(fragment) {
        info!(
            "GC: preserving fragment {} while a durable export is active",
            fragment
        );
        return stats;
    }

    let now = SystemTime::now();
    let now_unix = Utc::now().timestamp();
    let upload_scan = match crate::core::upload::UploadGcScan::begin(fragment) {
        Ok(scan) => scan,
        Err(error) => {
            record_gc_preservation_error(fragment, "cannot track concurrent uploads", error);
            return stats;
        }
    };
    let now_ts = Utc::now().timestamp();
    let ttl_secs = i64::try_from(CONFIG.garbage_collector.file_ttl_seconds).unwrap_or(i64::MAX);
    let mut active_session_versions = HashSet::<String>::new();
    let mut sessions_to_delete = Vec::<(String, u64, String)>::new();
    let active_sessions = match index::list_active_sessions(fragment).await {
        Ok(sessions) => sessions,
        Err(error) => {
            record_gc_preservation_error(
                fragment,
                "cannot establish durable upload protection",
                error,
            );
            return stats;
        }
    };
    for session in active_sessions {
        let idle_secs = now_ts.saturating_sub(session.last_activity_at.max(0));
        if idle_secs > ttl_secs {
            let amount = session.reservation.map(|r| r.amount).unwrap_or(0);
            sessions_to_delete.push((
                session.manifest.version_id,
                amount,
                session.manifest.object_key,
            ));
        } else {
            active_session_versions.insert(session.manifest.version_id);
        }
    }
    match crate::core::upload::active_standard_upload_versions(fragment) {
        Ok(versions) => active_session_versions.extend(versions),
        Err(error) => {
            record_gc_preservation_error(
                fragment,
                "cannot establish standard upload protection",
                error,
            );
            return stats;
        }
    }

    // Observe sessions before namespace references so captured sessions remain
    // protected while namespace publication proceeds. This ordering is not a
    // transactionally consistent snapshot of every concurrent mutation.
    // Do not begin destructive cleanup until both session sources were read.
    match global_namespace_runtime()
        .sweep_expired_trash(fragment, retention.as_secs(), now_unix)
        .await
    {
        Ok(summary) => stats.trash_versions_expired = summary.expired_versions,
        Err(err) => {
            record_gc_preservation_error(fragment, "trash lifecycle failed", err);
            return stats;
        }
    }
    let protected_object_ids = match global_namespace_runtime()
        .reachability_snapshot_if_present(fragment)
        .await
    {
        Ok(Some(snapshot)) => snapshot.referenced_object_ids,
        Ok(None) => {
            record_gc_preservation_error(
                fragment,
                "namespace reachability unavailable",
                "missing snapshot",
            );
            return stats;
        }
        Err(err) => {
            record_gc_preservation_error(fragment, "namespace reachability unavailable", err);
            return stats;
        }
    };

    for (version_id, amount, object_id) in &sessions_to_delete {
        io_progress.heartbeat();
        if should_stop_gc() {
            return stats;
        }
        let _reclaim_guard = match upload_scan.try_reclaim(object_id) {
            Ok(Some(guard)) => guard,
            Ok(None) => continue,
            Err(error) => {
                record_gc_preservation_error(fragment, "cannot fence expired session", error);
                return stats;
            }
        };
        if let Err(error) = index::remove_session(fragment, version_id, "gc-expired").await {
            record_gc_preservation_error(fragment, "cannot expire durable upload session", error);
            return stats;
        }
        stats.sessions_deleted = stats.sessions_deleted.saturating_add(1);
        stats.refund_bytes = stats.refund_bytes.saturating_add(*amount);
    }

    for root in blob_roots {
        io_progress.heartbeat();
        if should_stop_gc() {
            return stats;
        }
        let object_dirs = match index_store::local_object_dirs_under_root(root, fragment) {
            Ok(dirs) => dirs,
            Err(error) => {
                record_gc_preservation_error(fragment, "object directory scan failed", error);
                continue;
            }
        };
        for (object_dir, object_id) in object_dirs {
            io_progress.heartbeat();
            if should_stop_gc() {
                return stats;
            }
            let object_is_referenced = protected_object_ids.contains(&object_id);
            let _reclaim_guard = if object_is_referenced {
                None
            } else {
                match upload_scan.try_reclaim(&object_id) {
                    Ok(Some(guard)) => Some(guard),
                    Ok(None) => continue,
                    Err(error) => {
                        record_gc_preservation_error(
                            fragment,
                            "cannot fence orphan reclamation",
                            error,
                        );
                        return stats;
                    }
                }
            };
            if let Ok(entries) = std::fs::read_dir(&object_dir) {
                for entry in entries.filter_map(Result::ok) {
                    if should_stop_gc() {
                        return stats;
                    }
                    let path = entry.path();
                    if !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                        continue;
                    }
                    gc_cleanup_version_dir_unix(&path, now, file_ttl, is_manual_run);
                    if object_is_referenced {
                        stats.live_versions_seen = stats.live_versions_seen.saturating_add(1);
                    }
                }
            }
            if object_is_referenced {
                continue;
            }

            match reclaim_object_versions(
                &object_dir,
                &active_session_versions,
                now,
                file_ttl,
                || {
                    io_progress.heartbeat();
                    !should_stop_gc()
                },
            ) {
                Ok(removed) => stats.orphan_blob_dirs_deleted += removed,
                Err(error) => {
                    record_gc_preservation_error(fragment, "orphan reclamation incomplete", error)
                }
            }
        }
    }

    stats
}

#[cfg(test)]
mod tests {
    use super::{
        GcExecutionMode, GcRunOptions, collect_gc_fragments, orphan_version_reclaimable,
        parse_gc_execution_mode,
    };
    use crate::core::constants::FRAGMENT_METADATA_FILE;
    use crate::core::fragment_identity::{
        fragment_alias_dir, physical_fragment_dir_name, physical_fragment_root_on,
    };
    use std::fs;
    use std::os::unix::fs::symlink;
    use std::time::Duration;
    use ulid::Ulid;

    #[test]
    fn orphan_parent_pruning_preserves_unexpired_child() {
        let base = std::env::temp_dir().join(format!("n1-gc-parent-{}", Ulid::new()));
        let child = base.join(Ulid::new().to_string());
        fs::create_dir_all(&child).unwrap();
        fs::write(child.join("part_0_shard_0.shard"), b"keep upload bytes").unwrap();
        let now = std::time::SystemTime::now();
        fs::File::open(&base)
            .unwrap()
            .set_times(fs::FileTimes::new().set_modified(now - Duration::from_secs(120)))
            .unwrap();
        assert!(!super::prune_orphan_object_dir(
            &base,
            now,
            Duration::from_secs(60)
        ));
        assert_eq!(
            fs::read(child.join("part_0_shard_0.shard")).unwrap(),
            b"keep upload bytes"
        );
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn orphan_parent_pruning_preserves_unknown_files_even_in_scrub() {
        let base = std::env::temp_dir().join(format!("n1-gc-unknown-{}", Ulid::new()));
        fs::create_dir_all(&base).unwrap();
        fs::write(base.join("unknown.metadata"), b"preserve").unwrap();
        assert!(!super::prune_orphan_object_dir(
            &base,
            std::time::SystemTime::now(),
            Duration::ZERO
        ));
        assert_eq!(
            fs::read(base.join("unknown.metadata")).unwrap(),
            b"preserve"
        );
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn orphan_parent_pruning_removes_only_empty_expired_directories() {
        let base = std::env::temp_dir().join(format!("n1-gc-empty-{}", Ulid::new()));
        fs::create_dir_all(&base).unwrap();
        let now = std::time::SystemTime::now();
        assert!(!super::prune_orphan_object_dir(
            &base,
            now,
            Duration::from_secs(60)
        ));
        assert!(super::prune_orphan_object_dir(&base, now, Duration::ZERO));
        assert!(!base.exists());
    }

    #[test]
    fn orphan_parent_pruning_never_follows_symlinks() {
        let base = std::env::temp_dir().join(format!("n1-gc-symlink-{}", Ulid::new()));
        fs::create_dir_all(base.join("target")).unwrap();
        fs::write(base.join("target/payload"), b"not an orphan").unwrap();
        symlink(base.join("target"), base.join("link")).unwrap();
        assert!(!super::prune_orphan_object_dir(
            &base.join("link"),
            std::time::SystemTime::now(),
            Duration::ZERO
        ));
        assert_eq!(
            fs::read(base.join("target/payload")).unwrap(),
            b"not an orphan"
        );
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn orphan_version_reclaim_requires_valid_uncommitted_inactive_ulid_past_ttl() {
        let version_id = Ulid::new().to_string();
        let ttl = Duration::from_secs(60);
        assert!(!orphan_version_reclaimable(
            &version_id,
            true,
            false,
            Duration::from_secs(120),
            ttl,
        ));
        assert!(!orphan_version_reclaimable(
            &version_id,
            false,
            true,
            Duration::from_secs(120),
            ttl,
        ));
        assert!(!orphan_version_reclaimable(
            &version_id,
            false,
            false,
            Duration::from_secs(59),
            ttl,
        ));
        assert!(!orphan_version_reclaimable(
            "not-a-version",
            false,
            false,
            Duration::from_secs(120),
            ttl,
        ));
        assert!(orphan_version_reclaimable(
            &version_id,
            false,
            false,
            Duration::from_secs(60),
            ttl,
        ));
    }

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
        let stable_fragment = "stable-fragment";
        let real_fragment_dir = physical_fragment_root_on(&index_root, stable_fragment);
        fs::create_dir_all(&real_fragment_dir).expect("create fragment dir");
        fs::write(real_fragment_dir.join(FRAGMENT_METADATA_FILE), b"fragment")
            .expect("write fragment metadata marker");
        fs::create_dir_all(fragment_alias_dir(&index_root)).expect("create alias dir");
        symlink(
            std::path::PathBuf::from("..").join(physical_fragment_dir_name(stable_fragment)),
            fragment_alias_dir(&index_root).join(real_fragment),
        )
        .expect("create fragment alias");

        fs::create_dir_all(index_root.join(".Trash-1000")).expect("create desktop trash dir");
        fs::create_dir_all(index_root.join("lost+found")).expect("create unrelated dir");

        let fragments = collect_gc_fragments(&[index_root]);
        assert!(fragments.contains(stable_fragment));
        assert!(!fragments.contains(".Trash-1000"));
        assert!(!fragments.contains("lost+found"));

        let _ = fs::remove_dir_all(base);
    }
}
