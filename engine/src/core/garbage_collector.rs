use crate::core::constants::{FRAGMENT_METADATA_FILE, GC_SUFFIX};
use crate::core::disk_manager::ObjectPathManager;
use crate::core::fragment_identity::{
    fragment_root_on, list_fragment_aliases, stable_fragment_id_for_root,
};
use crate::core::index;
use crate::core::index_store;
use crate::core::io_scheduler::{DiskIoClass, DiskIoGuard, IoProgress, acquire_disk_io};
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

mod reclaim;
pub(crate) use reclaim::{object_directory_present, reclaim_object_versions};

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
        let configured_ttl = if is_manual_run {
            Duration::ZERO
        } else {
            Duration::from_secs(CONFIG.garbage_collector.file_ttl_seconds)
        };
        self.file_ttl_with_config(is_manual_run, configured_ttl)
    }

    fn file_ttl_with_config(self, is_manual_run: bool, configured_ttl: Duration) -> Duration {
        if !is_manual_run {
            return configured_ttl;
        }
        match self.execution_mode {
            GcExecutionMode::Availability if self.fast_mode => Duration::from_secs(5),
            GcExecutionMode::Availability => Duration::from_secs(1),
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

fn begin_gc_job(is_manual_run: bool, target_fragment: Option<&str>) -> bool {
    if GC_RUNNING.swap(true, Ordering::SeqCst) {
        return false;
    }
    GC_STOP_SIGNAL.store(false, Ordering::Relaxed);
    // Publish admission before returning 202 or beginning a scheduled run.
    // Both entry points must share the same mutual exclusion and status.
    with_gc_status_write(|st| {
        *st = GcJobStatus {
            is_running: true,
            phase: "Initializing".to_string(),
            is_manual_run,
            current_fragment: target_fragment.unwrap_or_default().to_string(),
            start_timestamp: Utc::now().timestamp(),
            ..Default::default()
        };
    });
    true
}

fn finalize_gc_status(status: &mut GcJobStatus, stopped: bool) {
    status.is_running = false;
    if stopped && status.phase != "Failed" {
        status.phase = "Stopped by User".to_string();
    } else if status.phase != "Failed" && status.phase != "Stopped by User" {
        status.phase = if status.total_errors > 0 {
            "Completed with errors"
        } else {
            "Completed"
        }
        .to_string();
    }
}

async fn run_gc_job(
    state: Arc<AppState>,
    is_manual_run: bool,
    target_fragment: Option<String>,
    options: GcRunOptions,
) {
    if let Err(err) = run_gc_scan_offthread(state, is_manual_run, target_fragment, options).await {
        error!("GC scan failed (manual={}): {}", is_manual_run, err);
        with_gc_status_write(|st| {
            st.total_errors = st.total_errors.saturating_add(1);
            st.last_error = Some(err.to_string());
            st.phase = "Failed".to_string();
        });
    }
    with_gc_status_write(|st| finalize_gc_status(st, should_stop_gc()));
    GC_STOP_SIGNAL.store(false, Ordering::Relaxed);
    GC_RUNNING.store(false, Ordering::SeqCst);
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
        if safety_read_only_reason().is_some() {
            sleep(cleanup_interval).await;
            continue;
        }
        if !begin_gc_job(false, None) {
            sleep(cleanup_interval).await;
            continue;
        }
        // Scheduled GC is the same conservative collector as manual GC, not
        // a separate trash-only loop. It also expires durable sessions and
        // discovers abandoned normal-upload versions using configured TTLs.
        run_gc_job(state.clone(), false, None, GcRunOptions::default()).await;
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
    if !begin_gc_job(true, target_fragment.as_deref()) {
        return (
            StatusCode::CONFLICT,
            "GC is already running. Check /noa/v1/maintenance/gc/status.".to_string(),
        );
    }
    tokio::spawn(async move {
        run_gc_job(state, true, target_fragment, options).await;
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
    cleanup_batch_temps: bool,
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

        let file_type = match ent.file_type() {
            Ok(kind) => kind,
            Err(_) => continue,
        };

        if is_batch_temporary_name(&name) {
            // Request-owned private metadata can remain below an otherwise
            // committed version after interruption. Reuse this existing pass,
            // under the caller's object reclaim guard. No immutable payload or
            // authority names match this exact private-file grammar.
            if cleanup_batch_temps
                && file_type.is_file()
                && version_dir
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| ulid::Ulid::from_string(name).is_ok())
            {
                if let Ok(metadata) = fs::symlink_metadata(&p) {
                    if metadata.file_type().is_file() {
                        if let Ok(modified) = metadata.modified() {
                            let age = now.duration_since(modified).unwrap_or(Duration::ZERO);
                            // Manual compatibility cleanup does not bypass the
                            // provided orphan TTL for private batch files.
                            if orphan_blob_age_expired(age, gc_ttl) {
                                let _ = fs::remove_file(&p);
                            }
                        }
                    }
                }
            }
            // Never follow batch-name symlinks or treat them as legacy links.
            continue;
        }

        if file_type.is_symlink() {
            symlinks.push(p);
            continue;
        }
        // Ordinary immutable shards do not need a metadata lookup merely
        // because GC visits their version directory for legacy artifacts.
        if !name.ends_with(GC_SUFFIX) && !name.contains(".preexisting.") {
            continue;
        }
        let md = match fs::symlink_metadata(&p) {
            Ok(metadata) => metadata,
            Err(_) => continue,
        };

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
    _cleanup_batch_temps: bool,
) {
}

fn is_batch_temporary_name(name: &str) -> bool {
    name.strip_prefix(".n1-batch-")
        .and_then(|name| name.strip_suffix(".tmp"))
        .is_some_and(|id| ulid::Ulid::from_string(id).is_ok_and(|parsed| parsed.to_string() == id))
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

fn reclaim_upload_candidate(path: &Path, now: SystemTime, ttl: Duration) -> std::io::Result<bool> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_dir() {
        return Ok(false);
    }
    let mut modified = metadata.modified()?;
    match fs::symlink_metadata(path.join("payload")) {
        Ok(payload) if payload.is_dir() => modified = modified.max(payload.modified()?),
        Ok(_) => return Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
        Err(error) => return Err(error),
    }
    let Ok(age) = now.duration_since(modified) else {
        return Ok(false);
    };
    if !orphan_blob_age_expired(age, ttl) {
        return Ok(false);
    }
    // This owner is private upload state. A move into .objects removes payload
    // from this subtree; deleting its remnants cannot traverse committed data.
    // Unknown controls or links preserve the owner for operator investigation.
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !((name == "payload" && kind.is_dir())
            || (name.starts_with(".n1-batch-") && name.ends_with(".tmp") && kind.is_file()))
        {
            return Ok(false);
        }
    }
    fs::remove_dir_all(path)?;
    Ok(true)
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

// Bound each traversal admission by entries AND elapsed work. A single slow
// syscall or object reclamation cannot be preempted, but it must not buy GC
// another 127 objects before foreground clients receive service.
const GC_OBJECTS_PER_ADMISSION: usize = 128;
const GC_ADMISSION_WORK_BUDGET: Duration = Duration::from_millis(32);
const GC_FOREGROUND_SERVICE_MAX: Duration = Duration::from_secs(1);

struct GcIoScope {
    guard: Option<DiskIoGuard>,
    fragment: Option<String>,
    objects_in_admission: usize,
    admission_started: std::time::Instant,
}

fn foreground_yield_duration(work: Duration, foreground_pending: bool) -> Duration {
    if !foreground_pending {
        Duration::ZERO
    } else {
        // Give scheduler-admitted foreground I/O a GC-free service window of
        // four work units. Include ACTIVE demand: an empty waiter
        // queue does not mean the physical drives are idle. The finite cap
        // permits another GC slice even under uninterrupted foreground load.
        work.saturating_mul(4)
            .clamp(GC_ADMISSION_WORK_BUDGET, GC_FOREGROUND_SERVICE_MAX)
    }
}

fn foreground_service_delay(remaining: Duration, foreground_pending: bool) -> Duration {
    if foreground_pending {
        remaining.min(GC_ADMISSION_WORK_BUDGET)
    } else {
        Duration::ZERO
    }
}

async fn foreground_service_window_with(
    pause: Duration,
    mut foreground_pending: impl FnMut() -> bool,
    mut stopped: impl FnMut() -> bool,
) -> bool {
    let deadline = tokio::time::Instant::now() + pause;
    loop {
        if stopped() {
            return false;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let delay = foreground_service_delay(remaining, foreground_pending());
        if delay.is_zero() {
            return true;
        }
        // Foreground can drain or GC can be stopped during its service window;
        // do not impose a full second of artificial delay on an idle server.
        sleep(delay).await;
    }
}

impl GcIoScope {
    async fn new(fragment: Option<&str>) -> Result<Option<Self>, AppError> {
        Ok(acquire_gc_io(fragment).await?.map(|guard| Self {
            guard: Some(guard),
            fragment: fragment.map(str::to_string),
            objects_in_admission: 0,
            admission_started: std::time::Instant::now(),
        }))
    }

    fn heartbeat(&self) {
        if let Some(guard) = &self.guard {
            guard.heartbeat();
        }
    }

    fn progress_handle(&self) -> IoProgress {
        self.guard
            .as_ref()
            .expect("GC work requires a scheduler admission")
            .progress_handle()
    }

    fn record_progress(&self, bytes: u64) {
        if let Some(guard) = &self.guard {
            guard.record_progress(bytes);
        }
    }

    fn rejoin_due(&self) -> bool {
        self.objects_in_admission.saturating_add(1) >= GC_OBJECTS_PER_ADMISSION
            || self.admission_started.elapsed() >= GC_ADMISSION_WORK_BUDGET
    }

    async fn cooperate(&mut self, fragment_boundary: bool) -> Result<bool, AppError> {
        self.cooperate_at_boundary(fragment_boundary || self.rejoin_due())
            .await
    }

    async fn cooperate_at_boundary(&mut self, rejoining: bool) -> Result<bool, AppError> {
        self.heartbeat();
        if should_stop_gc() {
            return Ok(false);
        }
        self.objects_in_admission = self.objects_in_admission.saturating_add(1);
        if !rejoining {
            return Ok(true);
        }
        let work = self.admission_started.elapsed();
        self.complete();
        let foreground = crate::core::io_scheduler::foreground_io_pending();
        // Returning a reserved background permit alone cannot give the same
        // physical drive a foreground-only service window. Give active OR
        // queued clients a bounded window; GC resumes even under steady load.
        let pause = foreground_yield_duration(work, foreground);
        if pause.is_zero() {
            tokio::task::yield_now().await;
        } else if !foreground_service_window_with(
            pause,
            crate::core::io_scheduler::foreground_io_pending,
            should_stop_gc,
        )
        .await
        {
            return Ok(false);
        }
        self.guard = acquire_gc_io(self.fragment.as_deref()).await?;
        self.objects_in_admission = 0;
        self.admission_started = std::time::Instant::now();
        Ok(self.guard.is_some())
    }

    fn complete(&mut self) {
        if let Some(guard) = self.guard.take() {
            guard.complete();
        }
    }
}

async fn acquire_gc_io(fragment: Option<&str>) -> Result<Option<DiskIoGuard>, AppError> {
    if should_stop_gc() {
        return Ok(None);
    }
    let admission = acquire_disk_io(DiskIoClass::BackgroundGc, fragment, "garbage collection");
    tokio::pin!(admission);
    loop {
        tokio::select! {
            result = &mut admission => return result.map(Some),
            _ = sleep(Duration::from_millis(100)) => {
                if should_stop_gc() {
                    return Ok(None);
                }
            }
        }
    }
}

async fn continue_fragment_gc(
    io_scope: &mut GcIoScope,
    export_guard: &mut Option<tokio::sync::MutexGuard<'static, ()>>,
    fragment: &str,
    blob_roots: &[PathBuf],
) -> Result<bool, AppError> {
    if let Some(reason) = safety_read_only_reason() {
        record_gc_preservation_error(fragment, "node entered safety read-only mode", reason);
        return Ok(false);
    }
    let rejoining = io_scope.rejoin_due();
    if rejoining {
        // Never wait for another I/O admission while preventing a new export
        // from being admitted. Recheck durable export protection afterwards.
        drop(export_guard.take());
    }
    // Use the exact same boundary decision that released export protection.
    // Rechecking elapsed time inside cooperate could cross the time budget
    // here and otherwise sleep/requeue while still holding that protection.
    if !io_scope.cooperate_at_boundary(rejoining).await? {
        return Ok(false);
    }
    if rejoining && !gc_members_available(fragment, blob_roots) {
        return Ok(false);
    }
    if export_guard.is_none() {
        *export_guard = crate::core::export::gc_reclaim_guard(fragment).await?;
    }
    Ok(export_guard.is_some())
}

async fn collect_fragment_references(
    fragment: &str,
    blob_roots: &[PathBuf],
    io_scope: &mut GcIoScope,
    export_guard: &mut Option<tokio::sync::MutexGuard<'static, ()>>,
) -> Result<Option<BTreeSet<String>>, AppError> {
    let mut scan = crate::namespace::flat::GcReferenceScan::new(fragment)?;
    let mut objects = BTreeSet::new();
    loop {
        // Physical directory/NCX reads can block on a busy HDD. Retain the
        // cursor across bounded blocking jobs instead of occupying an async
        // request worker or rebuilding a sorted inventory for every batch.
        let (next_scan, batch) = tokio::task::spawn_blocking(move || {
            let batch =
                scan.next_batch_with_budget(GC_OBJECTS_PER_ADMISSION, GC_ADMISSION_WORK_BUDGET);
            (scan, batch)
        })
        .await
        .map_err(|error| AppError::InternalServerError(format!("GC reference worker: {error}")))?;
        scan = next_scan;
        let Some(batch) = batch? else {
            break;
        };
        objects.extend(batch);
        io_scope.heartbeat();
        // Preparation yields after bounded physical entries too, not only
        // after later object reclamation. Never retain export admission while
        // queued for the next I/O batch; late exports still defer reclamation.
        io_scope.objects_in_admission = GC_OBJECTS_PER_ADMISSION - 1;
        if !continue_fragment_gc(io_scope, export_guard, fragment, blob_roots).await? {
            return Ok(None);
        }
    }
    Ok(Some(objects))
}

fn gc_members_available(fragment: &str, blob_roots: &[PathBuf]) -> bool {
    if let Some(reason) = safety_read_only_reason() {
        record_gc_preservation_error(fragment, "node is safety read-only", reason);
        return false;
    }
    if blob_roots.is_empty() {
        record_gc_preservation_error(fragment, "storage members unavailable", "no enrolled roots");
        return false;
    }
    if let Some(root) = blob_roots
        .iter()
        .find(|root| !crate::core::storage_pool::root_available(root))
    {
        record_gc_preservation_error(
            fragment,
            "storage member identity or availability cannot be proven",
            root.display(),
        );
        return false;
    }
    gc_fragment_roots_are_directories(fragment, blob_roots)
}

fn gc_fragment_roots_are_directories(fragment: &str, blob_roots: &[PathBuf]) -> bool {
    for root in blob_roots {
        let physical_root =
            crate::core::fragment_identity::physical_fragment_root_on(root, fragment);
        match fs::symlink_metadata(&physical_root) {
            Ok(metadata) if metadata.file_type().is_dir() => {}
            // A fragment need not exist in every storage pool. A missing
            // fragment is not permission to traverse a redirected one.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) => {
                record_gc_preservation_error(
                    fragment,
                    "physical fragment root is redirected or is not a directory",
                    physical_root.display(),
                );
                return false;
            }
            Err(error) => {
                record_gc_preservation_error(
                    fragment,
                    "cannot inspect physical fragment root",
                    error,
                );
                return false;
            }
        }
        for internal in [".trash", ".namespace", ".uploads", ".objects"] {
            let path = physical_root.join(internal);
            match fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.file_type().is_dir() => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Ok(_) => {
                    record_gc_preservation_error(
                        fragment,
                        "internal fragment root is redirected or is not a directory",
                        path.display(),
                    );
                    return false;
                }
                Err(error) => {
                    record_gc_preservation_error(
                        fragment,
                        "cannot inspect internal fragment root",
                        error,
                    );
                    return false;
                }
            }
        }
    }
    true
}

async fn perform_gc_scan(
    state: Arc<AppState>,
    is_manual_run: bool,
    target_fragment: Option<String>,
    options: GcRunOptions,
) -> Result<(), AppError> {
    let _maintenance_gate =
        acquire_maintenance_mode(options.execution_mode.maintenance_access_mode());
    let Some(mut io_scope) = GcIoScope::new(target_fragment.as_deref()).await? else {
        return Ok(());
    };
    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();
    // Foreground placement filters unavailable roots. Reclamation must not:
    // omitting a failed member could hide the only surviving reference and
    // turn an incomplete namespace view into destructive orphan evidence.
    let blob_roots = crate::core::disk_manager::BLOB_STORE_DISKS
        .get()
        .map(|roots| roots.iter().map(PathBuf::from).collect::<Vec<_>>())
        .unwrap_or_default();

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
            io_scope.complete();
            return Ok(());
        }
    };

    for fragment in fragments {
        if should_stop_gc() {
            break;
        }
        io_scope.heartbeat();
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
            &mut io_scope,
            &export_gc_protection,
        )
        .await?;
        io_scope.record_progress(stats.refund_bytes);

        with_gc_status_write(|st| {
            st.total_fragments_scanned += 1;
            st.total_live_versions_seen += stats.live_versions_seen;
            st.total_trash_versions_expired += stats.trash_versions_expired;
            st.total_sessions_deleted += stats.sessions_deleted;
            st.total_orphan_blob_dirs_deleted += stats.orphan_blob_dirs_deleted;
            st.total_refund_bytes += stats.refund_bytes;
            st.duration_seconds = start.elapsed().as_secs();
        });
        if !io_scope.cooperate(true).await? {
            break;
        }
    }

    if should_stop_gc() {
        info!("GC: Scan stopped by user request.");
        with_gc_status_write(|st| {
            st.phase = "Stopped by User".to_string();
            st.duration_seconds = start.elapsed().as_secs();
        });
        io_scope.complete();
        return Ok(());
    }

    info!("GC: Scan completed.");
    with_gc_status_write(|st| {
        st.phase = "Finalizing".to_string();
        st.duration_seconds = start.elapsed().as_secs();
    });
    io_scope.complete();
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
    io_scope: &mut GcIoScope,
    export_gc_protection: &HashSet<String>,
) -> Result<FragmentGcStats, AppError> {
    let mut stats = FragmentGcStats::default();
    if !gc_members_available(fragment, blob_roots) {
        return Ok(stats);
    }
    let _fragment_lifecycle = match crate::core::fragment_lifecycle::acquire_live(fragment).await {
        Ok(guard) => guard,
        Err(error) => {
            record_gc_preservation_error(fragment, "fragment lifecycle unavailable", error);
            return Ok(stats);
        }
    };

    match index_store::fragment_meta(fragment) {
        Ok(Some(_)) => {}
        Ok(None) => {
            record_gc_preservation_error(
                fragment,
                "engine metadata unavailable",
                "missing N1 3 metadata",
            );
            return Ok(stats);
        }
        Err(err) => {
            record_gc_preservation_error(fragment, "engine-format gate failed", err);
            return Ok(stats);
        }
    }

    if export_gc_protection.contains(fragment) {
        info!(
            "GC: preserving fragment {} while a durable export is active",
            fragment
        );
        return Ok(stats);
    }
    let mut export_guard = crate::core::export::gc_reclaim_guard(fragment).await?;
    if export_guard.is_none() {
        return Ok(stats);
    }

    let now = SystemTime::now();
    let now_unix = Utc::now().timestamp();
    let upload_scan = match crate::core::upload::UploadGcScan::begin(fragment) {
        Ok(scan) => scan,
        Err(error) => {
            record_gc_preservation_error(fragment, "cannot track concurrent uploads", error);
            return Ok(stats);
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
            return Ok(stats);
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
            return Ok(stats);
        }
    }

    for (version_id, amount, object_id) in &sessions_to_delete {
        if !continue_fragment_gc(io_scope, &mut export_guard, fragment, blob_roots).await? {
            return Ok(stats);
        }
        if should_stop_gc() {
            return Ok(stats);
        }
        let _reclaim_guard = match upload_scan.try_reclaim(object_id) {
            Ok(Some(guard)) => guard,
            Ok(None) => continue,
            Err(error) => {
                record_gc_preservation_error(fragment, "cannot fence expired session", error);
                return Ok(stats);
            }
        };
        if let Err(error) = index::remove_session(fragment, version_id, "gc-expired").await {
            record_gc_preservation_error(fragment, "cannot expire durable upload session", error);
            return Ok(stats);
        }
        stats.sessions_deleted = stats.sessions_deleted.saturating_add(1);
        stats.refund_bytes = stats.refund_bytes.saturating_add(*amount);
    }

    // Upload-owned payload has no NCX reachability dependency. Reclaim only
    // these candidates before walking the committed namespace. The separate
    // committed-orphan/trash pass below retains all existing protections.
    for root in blob_roots {
        let uploads = fragment_root_on(root, fragment).join(index_store::UPLOADS_DIR);
        let entries = match fs::read_dir(&uploads) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                record_gc_preservation_error(fragment, "upload candidate scan failed", error);
                continue;
            }
        };
        for entry in entries {
            if !continue_fragment_gc(io_scope, &mut export_guard, fragment, blob_roots).await? {
                return Ok(stats);
            }
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    record_gc_preservation_error(fragment, "upload candidate read failed", error);
                    continue;
                }
            };
            let version = entry.file_name().to_string_lossy().into_owned();
            if version.parse::<ulid::Ulid>().is_err()
                || active_session_versions.contains(&version)
                || !entry.file_type().is_ok_and(|kind| kind.is_dir())
            {
                continue;
            }
            let children = match fs::read_dir(entry.path()) {
                Ok(children) => children,
                Err(_) => continue,
            };
            for child in children {
                let Ok(child) = child else { continue };
                let Some(object) = child
                    .file_name()
                    .to_str()
                    .and_then(index_store::object_id_from_dir_name)
                else {
                    continue;
                };
                if child.file_name() != std::ffi::OsString::from(format!("@o_{object}")) {
                    continue;
                }
                if !child.file_type().is_ok_and(|kind| kind.is_dir()) {
                    continue;
                }
                let Some(_guard) = upload_scan.try_reclaim(&object)? else {
                    continue;
                };
                let candidate = child.path();
                let ttl = file_ttl;
                let removed = tokio::task::spawn_blocking(move || {
                    let _snapshot_boundary = crate::core::backup::barrier::mutation();
                    reclaim_upload_candidate(&candidate, now, ttl)
                })
                .await
                .map_err(|e| AppError::InternalServerError(format!("upload GC worker: {e}")))?;
                match removed {
                    Ok(true) => stats.orphan_blob_dirs_deleted += 1,
                    Ok(false) => (),
                    Err(error) => {
                        record_gc_preservation_error(fragment, "upload candidate preserved", error)
                    }
                }
            }
            // Never remove controls/receipts or unknown contents here.
            let _ = fs::remove_dir(entry.path());
        }
    }

    // Observe sessions before namespace references so captured sessions remain
    // protected while namespace publication proceeds. This ordering is not a
    // transactionally consistent snapshot of every concurrent mutation.
    // Do not begin destructive cleanup until both session sources were read.
    let trash_started = std::time::Instant::now();
    let trash_expiration = global_namespace_runtime()
        .sweep_expired_trash(fragment, retention.as_secs(), now_unix)
        .await;
    crate::io_control::record_io_latency(
        "gc.trash.expiration",
        trash_started.elapsed(),
        0,
        trash_expiration.is_ok(),
    );
    match trash_expiration {
        Ok(summary) => stats.trash_versions_expired = summary.expired_versions,
        Err(err) => {
            record_gc_preservation_error(fragment, "trash lifecycle failed", err);
            return Ok(stats);
        }
    }
    let references_started = std::time::Instant::now();
    let references =
        collect_fragment_references(fragment, blob_roots, io_scope, &mut export_guard).await;
    crate::io_control::record_io_latency(
        "gc.namespace.references",
        references_started.elapsed(),
        0,
        references.is_ok(),
    );
    let protected_object_ids = match references {
        Ok(Some(objects)) => objects,
        Ok(None) => {
            // Stop or late export admission deliberately deferred this pass.
            return Ok(stats);
        }
        Err(err) => {
            record_gc_preservation_error(fragment, "namespace reachability unavailable", err);
            return Ok(stats);
        }
    };

    for root in blob_roots {
        io_scope.heartbeat();
        if should_stop_gc() {
            return Ok(stats);
        }
        let mut object_dirs = match index_store::LocalObjectDirCursor::new(root, fragment) {
            Ok(dirs) => dirs,
            Err(error) => {
                record_gc_preservation_error(fragment, "object directory scan failed", error);
                continue;
            }
        };
        loop {
            if !continue_fragment_gc(io_scope, &mut export_guard, fragment, blob_roots).await? {
                return Ok(stats);
            }
            let (object_dir, object_id) = match object_dirs.next_object() {
                Ok(Some(object)) => object,
                Ok(None) => break,
                Err(error) => {
                    record_gc_preservation_error(fragment, "object directory scan failed", error);
                    break;
                }
            };
            let object_is_referenced = protected_object_ids.contains(&object_id);
            let _reclaim_guard = match upload_scan.try_reclaim(&object_id) {
                Ok(Some(guard)) => guard,
                Ok(None) => continue,
                Err(error) => {
                    record_gc_preservation_error(
                        fragment,
                        "cannot fence orphan reclamation",
                        error,
                    );
                    return Ok(stats);
                }
            };
            let io_progress = io_scope.progress_handle();
            // The reclaim helpers inspect compatibility artifacts during their
            // existing version-directory pass, not a separate object walk.
            // Explicit manual GC retains its existing immediate-artifact
            // cleanup policy, independent of the configured orphan TTL.
            if is_manual_run {
                if let Ok(entries) = fs::read_dir(&object_dir) {
                    for entry in entries.filter_map(Result::ok) {
                        io_progress.heartbeat();
                        if should_stop_gc() {
                            return Ok(stats);
                        }
                        if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                            let version = entry.file_name().to_string_lossy().into_owned();
                            gc_cleanup_version_dir_unix(
                                &entry.path(),
                                now,
                                file_ttl,
                                true,
                                !active_session_versions.contains(&version),
                            );
                        }
                    }
                }
            }
            if object_is_referenced {
                match reclaim::reclaim_unpublished_versions(
                    fragment,
                    &object_id,
                    &object_dir,
                    &active_session_versions,
                    now,
                    file_ttl,
                    || {
                        io_progress.heartbeat();
                        !should_stop_gc()
                    },
                ) {
                    Ok((removed, live_seen)) => {
                        stats.orphan_blob_dirs_deleted += removed;
                        stats.live_versions_seen += live_seen;
                    }
                    Err(error) => record_gc_preservation_error(
                        fragment,
                        "unpublished-version reclamation incomplete",
                        error,
                    ),
                }
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

    Ok(stats)
}

#[cfg(test)]
mod tests {
    #[test]
    fn upload_candidate_cleanup_preserves_committed_payload_unknown_controls_and_redirects() {
        use super::reclaim_upload_candidate;
        use std::{
            fs,
            time::{Duration, SystemTime},
        };
        let root = std::env::temp_dir().join(format!("n1-upload-candidate-{}", ulid::Ulid::new()));
        let committed = root.join(".objects/object/version");
        let candidate = root.join(".uploads/version/@o_object");
        fs::create_dir_all(&committed).unwrap();
        fs::write(committed.join("shard"), b"immutable").unwrap();
        fs::create_dir_all(candidate.join("payload")).unwrap();
        fs::write(candidate.join("payload/shard"), b"unfinished").unwrap();
        assert!(
            !reclaim_upload_candidate(&candidate, SystemTime::now(), Duration::from_secs(3600))
                .unwrap()
        );
        fs::write(candidate.join("unknown-control"), b"preserve").unwrap();
        assert!(!reclaim_upload_candidate(&candidate, SystemTime::now(), Duration::ZERO).unwrap());
        fs::remove_file(candidate.join("unknown-control")).unwrap();
        assert!(reclaim_upload_candidate(&candidate, SystemTime::now(), Duration::ZERO).unwrap());
        assert_eq!(fs::read(committed.join("shard")).unwrap(), b"immutable");
        std::os::unix::fs::symlink(&committed, &candidate).unwrap();
        assert!(!reclaim_upload_candidate(&candidate, SystemTime::now(), Duration::ZERO).unwrap());
        assert_eq!(fs::read(committed.join("shard")).unwrap(), b"immutable");
        fs::remove_dir_all(root).unwrap();
    }
    use super::{
        GcExecutionMode, GcJobStatus, GcRunOptions, collect_gc_fragments,
        orphan_version_reclaimable, parse_gc_execution_mode,
    };
    use crate::core::constants::FRAGMENT_METADATA_FILE;
    use crate::core::fragment_identity::{
        fragment_alias_dir, physical_fragment_dir_name, physical_fragment_root_on,
    };
    use std::fs;

    #[test]
    fn gc_foreground_service_window_is_bounded_and_never_disables_collection() {
        assert_eq!(
            super::foreground_yield_duration(Duration::from_secs(5), false),
            Duration::ZERO
        );
        assert_eq!(
            super::foreground_yield_duration(Duration::from_millis(10), true),
            Duration::from_millis(40)
        );
        assert_eq!(
            super::foreground_yield_duration(Duration::from_secs(30), true),
            super::GC_FOREGROUND_SERVICE_MAX
        );
        assert_eq!(
            super::foreground_yield_duration(Duration::ZERO, true),
            super::GC_ADMISSION_WORK_BUDGET
        );
    }

    #[test]
    fn gc_continuous_foreground_gets_service_without_starving_collection() {
        // Advance a deterministic clock through repeated slices with demand
        // continuously present. A busy client cannot reset GC's wake deadline.
        let work = Duration::from_millis(32);
        let mut elapsed = Duration::ZERO;
        let mut completed_gc_slices = 0;
        for _ in 0..100 {
            elapsed += work;
            let mut remaining = super::foreground_yield_duration(work, true);
            let mut polls = 0;
            loop {
                let delay = super::foreground_service_delay(remaining, true);
                if delay.is_zero() {
                    break;
                }
                assert!(delay <= super::GC_ADMISSION_WORK_BUDGET);
                elapsed += delay;
                remaining = remaining.saturating_sub(delay);
                polls += 1;
                assert!(polls <= 32, "GC must regain service under steady demand");
            }
            completed_gc_slices += 1;
        }
        assert_eq!(completed_gc_slices, 100);
        assert_eq!(elapsed, work * 500);
    }

    #[test]
    fn gc_foreground_window_ends_as_soon_as_demand_clears() {
        assert_eq!(
            super::foreground_service_delay(super::GC_FOREGROUND_SERVICE_MAX, false),
            Duration::ZERO
        );
        assert_eq!(
            super::foreground_service_delay(Duration::ZERO, true),
            Duration::ZERO
        );
        assert_eq!(
            super::foreground_service_delay(super::GC_FOREGROUND_SERVICE_MAX, true),
            super::GC_ADMISSION_WORK_BUDGET
        );
    }

    #[tokio::test]
    async fn gc_service_wait_checks_stop_and_idle_before_sleeping() {
        assert!(
            super::foreground_service_window_with(
                super::GC_FOREGROUND_SERVICE_MAX,
                || false,
                || false,
            )
            .await
        );
        assert!(
            !super::foreground_service_window_with(
                super::GC_FOREGROUND_SERVICE_MAX,
                || panic!("stopping must not inspect demand or wait"),
                || true,
            )
            .await
        );
    }

    #[test]
    fn gc_rejoins_after_slow_work_even_before_count_limit() {
        let mut scope = super::GcIoScope {
            guard: None,
            fragment: None,
            objects_in_admission: 1,
            admission_started: std::time::Instant::now(),
        };
        scope.admission_started -= super::GC_ADMISSION_WORK_BUDGET;
        assert!(scope.rejoin_due());
        scope.admission_started = std::time::Instant::now();
        scope.objects_in_admission = super::GC_OBJECTS_PER_ADMISSION - 1;
        assert!(scope.rejoin_due());
    }
    use std::os::unix::fs::symlink;
    use std::time::Duration;
    use ulid::Ulid;

    #[test]
    fn gc_defers_reclamation_when_any_storage_member_is_missing_or_restore_pending() {
        let base = std::env::temp_dir().join(format!("n1-gc-member-safety-{}", Ulid::new()));
        fs::create_dir_all(&base).unwrap();
        let missing = base.join("missing-member");
        assert!(!crate::core::storage_pool::root_available(&missing));
        assert!(!super::gc_members_available("test-fragment", &[missing]));
        let pending = base.join("restore-pending-member");
        fs::create_dir_all(&pending).unwrap();
        fs::write(
            pending.join(crate::core::instance_lease::RESTORE_PENDING),
            b"restore not yet verified",
        )
        .unwrap();
        assert!(!crate::core::storage_pool::root_available(&pending));
        assert!(!super::gc_members_available("test-fragment", &[pending]));
        assert!(!super::gc_members_available("test-fragment", &[]));
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn gc_rejects_redirected_or_non_directory_fragment_roots_before_trash_cleanup() {
        let base = std::env::temp_dir().join(format!("n1-gc-fragment-root-{}", Ulid::new()));
        let roots = vec![base.join("member-a"), base.join("member-b")];
        for root in &roots {
            fs::create_dir_all(root).unwrap();
        }
        let fragment = Ulid::new().to_string();
        let physical_a = physical_fragment_root_on(&roots[0], &fragment);
        let physical_b = physical_fragment_root_on(&roots[1], &fragment);
        fs::create_dir_all(&physical_a).unwrap();
        assert!(super::gc_fragment_roots_are_directories(&fragment, &roots));

        let outside = base.join("outside-fragment");
        fs::create_dir_all(outside.join(".trash")).unwrap();
        let preserved = outside.join(".trash/must-not-be-reclaimed.ncx");
        fs::write(&preserved, b"outside enrolled physical fragment").unwrap();
        symlink(&outside, &physical_b).unwrap();
        assert!(!super::gc_fragment_roots_are_directories(&fragment, &roots));
        assert_eq!(
            fs::read(&preserved).unwrap(),
            b"outside enrolled physical fragment"
        );
        fs::remove_file(&physical_b).unwrap();

        fs::write(&physical_b, b"not a directory").unwrap();
        assert!(!super::gc_fragment_roots_are_directories(&fragment, &roots));
        fs::remove_file(&physical_b).unwrap();
        symlink(base.join("missing-outside-target"), &physical_b).unwrap();
        assert!(!super::gc_fragment_roots_are_directories(&fragment, &roots));
        fs::remove_file(&physical_b).unwrap();
        fs::create_dir_all(&physical_b).unwrap();
        assert!(super::gc_fragment_roots_are_directories(&fragment, &roots));
        for internal in [".trash", ".namespace", ".uploads", ".objects"] {
            let child = physical_b.join(internal);
            symlink(&outside, &child).unwrap();
            assert!(
                !super::gc_fragment_roots_are_directories(&fragment, &roots),
                "redirected {internal} must prevent destructive fragment traversal"
            );
            assert_eq!(
                fs::read(&preserved).unwrap(),
                b"outside enrolled physical fragment"
            );
            fs::remove_file(&child).unwrap();
            fs::write(&child, b"not a directory").unwrap();
            assert!(!super::gc_fragment_roots_are_directories(&fragment, &roots));
            fs::remove_file(&child).unwrap();
            fs::create_dir_all(&child).unwrap();
            assert!(super::gc_fragment_roots_are_directories(&fragment, &roots));
        }
        fs::remove_dir_all(base).unwrap();
    }

    #[tokio::test]
    async fn gc_releases_export_protection_before_queued_io_and_defers_for_late_export() {
        use crate::core::io_scheduler::{DiskIoClass, acquire_disk_io, disk_io_scheduler_config};
        use std::sync::atomic::Ordering;
        use tokio::time::timeout;

        let _global = crate::test_support::global_test_lock().lock().await;
        super::GC_STOP_SIGNAL.store(false, Ordering::Relaxed);
        crate::core::disk_manager::FS_STORAGE_ROOT.get_or_init(|| {
            std::env::temp_dir()
                .join(format!("n1-matrix-{}", std::process::id()))
                .to_string_lossy()
                .into_owned()
        });
        let fragment = format!("gc-late-export-{}", Ulid::new());
        let fixture = std::env::temp_dir().join(format!("n1-gc-queued-{}", Ulid::new()));
        fs::create_dir_all(&fixture).unwrap();
        assert!(crate::core::storage_pool::root_available(&fixture));

        let mut io_scope = super::GcIoScope::new(Some(&fragment))
            .await
            .unwrap()
            .unwrap();
        // Exercise the elapsed-time boundary, not only a full 128-object
        // admission: a slow first object must release export protection too.
        io_scope.objects_in_admission = 1;
        io_scope.admission_started -= super::GC_ADMISSION_WORK_BUDGET;
        let export_guard = crate::core::export::gc_reclaim_guard(&fragment)
            .await
            .unwrap();
        assert!(export_guard.is_some());
        let mut blockers = Vec::new();
        for _ in 1..disk_io_scheduler_config().background_per_fragment_permits {
            blockers.push(
                acquire_disk_io(
                    DiskIoClass::BackgroundGc,
                    Some(&fragment),
                    "GC regression blocker",
                )
                .await
                .unwrap(),
            );
        }
        let (blocker_acquired_tx, blocker_acquired_rx) = tokio::sync::oneshot::channel();
        let (release_blocker_tx, release_blocker_rx) = tokio::sync::oneshot::channel();
        let blocker_fragment = fragment.clone();
        let blocker = tokio::spawn(async move {
            let guard = acquire_disk_io(
                DiskIoClass::BackgroundGc,
                Some(&blocker_fragment),
                "queued ahead of GC re-admission",
            )
            .await
            .unwrap();
            blocker_acquired_tx.send(()).unwrap();
            let _ = release_blocker_rx.await;
            guard.complete();
        });
        timeout(Duration::from_secs(5), async {
            loop {
                if crate::core::io_scheduler::disk_io_scheduler_snapshot().queued_background > 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("blocker must queue behind the current GC admission");

        let gc_fragment = fragment.clone();
        let gc_root = fixture.clone();
        let collector = tokio::spawn(async move {
            let mut export_guard = export_guard;
            let result = super::continue_fragment_gc(
                &mut io_scope,
                &mut export_guard,
                &gc_fragment,
                &[gc_root],
            )
            .await;
            io_scope.complete();
            (result, export_guard.is_none())
        });
        timeout(Duration::from_secs(5), blocker_acquired_rx)
            .await
            .unwrap()
            .unwrap();
        assert!(
            !collector.is_finished(),
            "GC must be queued behind the admitted blocker"
        );
        let unrelated = format!("late-export-admission-{}", Ulid::new());
        let admission_guard = timeout(
            Duration::from_secs(5),
            crate::core::export::gc_reclaim_guard(&unrelated),
        )
        .await
        .expect(
            "GC must release the existing export quota lock BEFORE waiting for I/O re-admission",
        )
        .unwrap()
        .unwrap();
        assert!(!collector.is_finished());

        // Publish a valid, test-owned queued export under the same admission
        // lock production export creation uses. No export worker is needed.
        let export_id = Ulid::new().to_string();
        let job_directory =
            std::path::Path::new(crate::core::disk_manager::FS_STORAGE_ROOT.get().unwrap())
                .join(".n1-exports")
                .join(&export_id);
        fs::create_dir_all(&job_directory).unwrap();
        let state = serde_json::json!({
            "id": export_id,
            "fragment": fragment,
            "request": {
                "keys": ["source.bin"], "entries": [], "prefix": null,
                "prefixes": [], "format": "tar", "filename": null,
                "export_all": false, "include_internal": false
            },
            "phase": "queued", "format": "tar", "filename": "export.tar",
            "created_at_unix": 1, "updated_at_unix": 1,
            "expires_at_unix": chrono::Utc::now().timestamp() + 3600
        });
        fs::write(
            job_directory.join("state.json"),
            serde_json::to_vec(&state).unwrap(),
        )
        .unwrap();
        drop(admission_guard);
        release_blocker_tx.send(()).unwrap();
        timeout(Duration::from_secs(5), blocker)
            .await
            .unwrap()
            .unwrap();
        let (continued, guard_released) = timeout(Duration::from_secs(5), collector)
            .await
            .unwrap()
            .unwrap();
        assert!(
            !continued.unwrap(),
            "late export protection must defer the next reclaim batch"
        );
        assert!(guard_released);
        for guard in blockers {
            guard.complete();
        }
        fs::remove_dir_all(job_directory).unwrap();
        fs::remove_dir_all(fixture).unwrap();
    }

    #[test]
    fn compatibility_cleanup_expires_redistribution_artifacts_without_touching_shards() {
        let version = std::env::temp_dir().join(format!("n1-gc-compatibility-{}", Ulid::new()));
        fs::create_dir_all(&version).unwrap();
        let now = std::time::SystemTime::now();
        let old = now - Duration::from_secs(120);
        let shard = version.join("part_0_shard_0.shard");
        let retired = version.join("part_0_shard_1.shard.gc");
        let preexisting = version.join("part_0_shard_2.shard.preexisting.old");
        let fresh_preexisting = version.join("part_0_shard_3.shard.preexisting.fresh");
        fs::write(&shard, b"immutable committed payload").unwrap();
        fs::write(&retired, b"retired redistribution piece").unwrap();
        fs::write(&preexisting, b"expired replaced piece").unwrap();
        fs::write(&fresh_preexisting, b"still protected by TTL").unwrap();
        for file in [&retired, &preexisting] {
            fs::File::open(file)
                .unwrap()
                .set_times(fs::FileTimes::new().set_modified(old))
                .unwrap();
        }
        let shard_before = fs::metadata(&shard).unwrap().modified().unwrap();
        super::gc_cleanup_version_dir_unix(&version, now, Duration::from_secs(60), false, true);
        assert!(!retired.exists());
        assert!(!preexisting.exists());
        assert_eq!(
            fs::read(&fresh_preexisting).unwrap(),
            b"still protected by TTL"
        );
        assert_eq!(fs::read(&shard).unwrap(), b"immutable committed payload");
        assert_eq!(
            fs::metadata(&shard).unwrap().modified().unwrap(),
            shard_before
        );
        fs::remove_dir_all(version).unwrap();
    }

    #[test]
    fn scheduled_gc_uses_availability_and_never_shortens_configured_orphan_ttl() {
        let scheduled = GcRunOptions::default();
        assert_eq!(scheduled.execution_mode, GcExecutionMode::Availability);
        assert!(!scheduled.fast_mode);
        for configured in [
            Duration::ZERO,
            Duration::from_secs(60),
            Duration::from_secs(86_400),
        ] {
            for execution_mode in [
                GcExecutionMode::Availability,
                GcExecutionMode::Strict,
                GcExecutionMode::Scrub,
            ] {
                for fast_mode in [false, true] {
                    assert_eq!(
                        GcRunOptions {
                            execution_mode,
                            fast_mode,
                        }
                        .file_ttl_with_config(false, configured),
                        configured,
                        "scheduled collection must not inherit aggressive manual TTLs",
                    );
                }
            }
        }
    }

    #[test]
    fn shared_gc_finalization_preserves_failure_stop_and_error_results() {
        for (phase, errors, stopped, expected) in [
            ("Finalizing", 0, false, "Completed"),
            ("Finalizing", 1, false, "Completed with errors"),
            ("Processing fragment", 0, true, "Stopped by User"),
            ("Stopped by User", 0, false, "Stopped by User"),
            ("Failed", 1, false, "Failed"),
            ("Failed", 1, true, "Failed"),
        ] {
            for is_manual_run in [false, true] {
                let mut status = GcJobStatus {
                    is_running: true,
                    phase: phase.to_string(),
                    total_errors: errors,
                    is_manual_run,
                    ..Default::default()
                };
                super::finalize_gc_status(&mut status, stopped);
                assert!(!status.is_running);
                assert_eq!(status.phase, expected);
                assert_eq!(status.is_manual_run, is_manual_run);
            }
        }
    }

    #[tokio::test]
    async fn scheduled_and_manual_gc_share_admission_and_count_cooperative_io_units() {
        use std::sync::atomic::Ordering;

        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                super::GC_STOP_SIGNAL.store(false, Ordering::Relaxed);
                super::GC_RUNNING.store(false, Ordering::SeqCst);
            }
        }
        let _reset = Reset;
        assert!(super::begin_gc_job(false, None));
        let admitted = super::gc_status_read_clone();
        assert!(admitted.is_running);
        assert!(!admitted.is_manual_run);
        assert_eq!(admitted.phase, "Initializing");
        assert!(!super::begin_gc_job(true, Some("manual-conflict")));
        assert!(!super::gc_status_read_clone().is_manual_run);

        let fragment = format!("gc-admission-test-{}", Ulid::new());
        let completed_units = || {
            crate::core::io_scheduler::disk_io_scheduler_snapshot()
                .classes
                .into_iter()
                .find(|class| class.class == "background_gc")
                .unwrap()
                .completed
        };
        let completed_before = completed_units();
        let mut scope = super::GcIoScope::new(Some(&fragment))
            .await
            .unwrap()
            .unwrap();
        let own_operation = || {
            crate::core::io_scheduler::disk_io_scheduler_snapshot()
                .active_operations
                .into_iter()
                .find(|operation| operation.fragment.as_deref() == Some(fragment.as_str()))
        };
        let first = own_operation().expect("background GC admission is observable");
        assert_eq!(first.class, "background_gc");
        for _ in 1..super::GC_OBJECTS_PER_ADMISSION {
            // Count-only regression: elapsed-time admission has its own test.
            // Explicit decisions avoid wall-clock flakiness under CI load.
            assert!(scope.cooperate_at_boundary(false).await.unwrap());
            assert_eq!(own_operation().unwrap().operation_id, first.operation_id);
        }
        assert!(
            scope
                .cooperate_at_boundary(scope.rejoin_due())
                .await
                .unwrap()
        );
        assert_ne!(own_operation().unwrap().operation_id, first.operation_id);
        scope.complete();
        assert!(own_operation().is_none());
        assert!(completed_units() >= completed_before + 2);

        super::GC_STOP_SIGNAL.store(true, Ordering::Relaxed);
        assert!(
            super::acquire_gc_io(Some(&fragment))
                .await
                .unwrap()
                .is_none()
        );
        assert!(own_operation().is_none());
    }

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
