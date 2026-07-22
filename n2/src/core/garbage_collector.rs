use crate::core::constants::FRAGMENT_METADATA_FILE;
use crate::core::disk_manager::ObjectPathManager;
use crate::core::fragment_identity::{
    list_fragment_aliases, physical_fragment_root_on, stable_fragment_id_for_root,
};
use crate::core::fragment_metadata::resolve_stable_fragment_id;
use crate::core::index;
use crate::core::operation_gate::{MaintenanceAccessMode, acquire_maintenance_mode};
use crate::distribution::lifecycle::purge_object_from_holders;
use crate::distribution::ncnx::{
    list_canonical_names, ncnx_cursor, read_canonical_record, remove_trash,
};
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
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, RwLock};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};

const PAGE_SIZE: usize = 1_000;

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
    live_objects_seen: u64,
    trash_versions_expired: u64,
    sessions_deleted: u64,
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

static GC_STATUS: LazyLock<Arc<RwLock<GcJobStatus>>> =
    LazyLock::new(|| Arc::new(RwLock::new(GcJobStatus::default())));
static GC_RUNNING: LazyLock<AtomicBool> = LazyLock::new(|| AtomicBool::new(false));
static GC_STOP_SIGNAL: LazyLock<AtomicBool> = LazyLock::new(|| AtomicBool::new(false));

fn should_stop_gc() -> bool {
    GC_STOP_SIGNAL.load(Ordering::Relaxed)
}

fn status_snapshot() -> GcJobStatus {
    GC_STATUS
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

fn update_status(update: impl FnOnce(&mut GcJobStatus)) {
    let mut status = GC_STATUS
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    update(&mut status);
}

pub async fn get_gc_status() -> GcJobStatus {
    status_snapshot()
}

pub async fn get_gc_status_api(claims: Claims) -> Result<Json<GcJobStatus>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    Ok(Json(status_snapshot()))
}

fn parse_gc_execution_mode(params: &HashMap<String, String>) -> GcExecutionMode {
    match params
        .get("mode")
        .or_else(|| params.get("execution_mode"))
        .or_else(|| params.get("access_mode"))
        .map(|value| value.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("strict") | Some("readonly") | Some("read_only") => GcExecutionMode::Strict,
        Some("scrub") | Some("quiesced") | Some("offline") => GcExecutionMode::Scrub,
        _ => GcExecutionMode::Availability,
    }
}

fn parse_bool_param(params: &HashMap<String, String>, key: &str, default: bool) -> bool {
    params
        .get(key)
        .map(|value| value.trim().to_ascii_lowercase())
        .map(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on" | "fast"))
        .unwrap_or(default)
}

fn collect_gc_fragments(roots: &[PathBuf]) -> HashSet<String> {
    let mut fragments = HashSet::new();
    for root in roots {
        for alias in list_fragment_aliases(root) {
            let Some(stable) = stable_fragment_id_for_root(root, &alias) else {
                continue;
            };
            if roots.iter().any(|candidate| {
                physical_fragment_root_on(candidate, &stable)
                    .join(FRAGMENT_METADATA_FILE)
                    .is_file()
            }) {
                fragments.insert(stable);
            }
        }
    }
    fragments
}

async fn collect_live_object_ids(fragment: &str) -> Result<BTreeSet<String>, AppError> {
    let mut object_ids = BTreeSet::new();
    let mut cursor = None;
    loop {
        let page = global_namespace_runtime()
            .list_live_object_refs_page(fragment, None, cursor.as_deref(), PAGE_SIZE)
            .await?;
        object_ids.extend(page.entries.into_iter().map(|entry| entry.object_id));
        let Some(next) = page.continuation_token else {
            break;
        };
        cursor = Some(next);
    }
    Ok(object_ids)
}

async fn collect_unexpired_trash_object_ids(
    fragment: &str,
    retention: Duration,
    now_unix: i64,
) -> Result<BTreeSet<String>, AppError> {
    let mut protected = BTreeSet::new();
    let mut cursor = None;
    loop {
        let page = list_canonical_names(fragment, "", cursor.as_deref(), PAGE_SIZE, true).await?;
        if page.is_empty() {
            break;
        }
        cursor = page
            .last()
            .map(|name| ncnx_cursor(&name.logical_key, name.trash.as_ref()));
        let full = page.len() == PAGE_SIZE;
        for name in page {
            let Some(trash) = name.trash.as_ref() else {
                continue;
            };
            let deleted_at = trash.deleted_at_unix_nanos / 1_000_000_000;
            let age = Duration::from_secs(now_unix.saturating_sub(deleted_at.max(0)).max(0) as u64);
            if age < retention {
                protected.insert(
                    read_canonical_record(fragment, &name)
                        .await?
                        .record
                        .object_id,
                );
            }
        }
        if !full {
            break;
        }
    }
    Ok(protected)
}

async fn reclaim_expired_trash(
    fragment: &str,
    retention: Duration,
    now_unix: i64,
    protected: &BTreeSet<String>,
    stats: &mut FragmentGcStats,
) -> Result<(), AppError> {
    let mut reclaimed_objects = BTreeSet::new();
    let mut cursor = None;
    loop {
        let page = list_canonical_names(fragment, "", cursor.as_deref(), PAGE_SIZE, true).await?;
        if page.is_empty() {
            break;
        }
        cursor = page
            .last()
            .map(|name| ncnx_cursor(&name.logical_key, name.trash.as_ref()));
        let full = page.len() == PAGE_SIZE;
        for name in page {
            if should_stop_gc() {
                return Ok(());
            }
            let Some(trash) = name.trash.as_ref() else {
                continue;
            };
            let deleted_at = trash.deleted_at_unix_nanos / 1_000_000_000;
            let age = Duration::from_secs(now_unix.saturating_sub(deleted_at.max(0)).max(0) as u64);
            if age < retention {
                continue;
            }
            let located = read_canonical_record(fragment, &name).await?;
            let object_id = located.record.object_id.clone();

            // Any live key or any still-recoverable trash key protects the
            // immutable object. Only this expired pointer is then discarded.
            if protected.contains(&object_id) {
                remove_trash(&located, trash).await?;
                continue;
            }

            if !reclaimed_objects.contains(&object_id) {
                let versions =
                    match index::list_live_object_versions_metadata(fragment, &object_id).await {
                        Ok(versions) => versions,
                        Err(AppError::NotFound) => Vec::new(),
                        Err(error) => return Err(error),
                    };
                let refund = versions
                    .iter()
                    .map(|metadata| metadata.total_size_bytes)
                    .sum::<u64>();
                let lifecycle_id = format!("gc-{}-{now_unix}", trash.delete_mutation_id);
                purge_object_from_holders(fragment, &object_id, &lifecycle_id).await?;
                reclaimed_objects.insert(object_id);
                stats.refund_bytes = stats.refund_bytes.saturating_add(refund);
                stats.trash_versions_expired = stats
                    .trash_versions_expired
                    .saturating_add(versions.len().max(1) as u64);
            }
            // Object holders are reclaimed first. Keeping NCNX on any failure
            // makes the entire operation retryable after restart.
            remove_trash(&located, trash).await?;
        }
        if !full {
            break;
        }
    }
    Ok(())
}

async fn expire_upload_sessions(
    fragment: &str,
    now_unix: i64,
    stats: &mut FragmentGcStats,
) -> Result<(), AppError> {
    let ttl = CONFIG.garbage_collector.file_ttl_seconds as i64;
    for session in index::list_active_sessions(fragment).await? {
        if should_stop_gc() {
            break;
        }
        if now_unix.saturating_sub(session.last_activity_at.max(0)) <= ttl {
            continue;
        }
        let refund = session
            .reservation
            .map(|reservation| reservation.amount)
            .unwrap_or(0);
        index::remove_session(fragment, &session.manifest.version_id, "gc-expired").await?;
        stats.sessions_deleted = stats.sessions_deleted.saturating_add(1);
        stats.refund_bytes = stats.refund_bytes.saturating_add(refund);
    }
    Ok(())
}

async fn process_fragment_gc(
    fragment: &str,
    retention: Duration,
) -> Result<FragmentGcStats, AppError> {
    let now_unix = Utc::now().timestamp();
    let live = collect_live_object_ids(fragment).await?;
    let mut protected = collect_unexpired_trash_object_ids(fragment, retention, now_unix).await?;
    protected.extend(live.iter().cloned());

    let mut stats = FragmentGcStats {
        live_objects_seen: live.len() as u64,
        ..FragmentGcStats::default()
    };
    reclaim_expired_trash(fragment, retention, now_unix, &protected, &mut stats).await?;
    expire_upload_sessions(fragment, now_unix, &mut stats).await?;
    Ok(stats)
}

async fn perform_gc_scan(
    is_manual_run: bool,
    target_fragment: Option<String>,
    options: GcRunOptions,
) -> Result<(), AppError> {
    let _maintenance = acquire_maintenance_mode(options.execution_mode.maintenance_access_mode());
    let roots = ObjectPathManager::new().get_storage_roots();
    let mut fragments = collect_gc_fragments(&roots).into_iter().collect::<Vec<_>>();
    fragments.sort();
    if let Some(target) = target_fragment {
        if !fragments.iter().any(|fragment| fragment == &target) {
            return Err(AppError::NotFound);
        }
        fragments.retain(|fragment| fragment == &target);
    }
    let retention = if options.fast_mode && is_manual_run {
        Duration::ZERO
    } else {
        Duration::from_secs(CONFIG.garbage_collector.retention_seconds)
    };
    let started = std::time::Instant::now();
    update_status(|status| {
        status.total_fragments = fragments.len() as u64;
        status.phase = format!("scanning_ncnx_trash:{}", options.execution_mode.as_str());
    });
    info!(
        fragments = fragments.len(),
        mode = options.execution_mode.as_str(),
        retention_seconds = retention.as_secs(),
        "N2 3.1 NCNX generation garbage collection started"
    );

    for fragment in fragments {
        if should_stop_gc() {
            update_status(|status| status.phase = "Stopped by User".into());
            break;
        }
        update_status(|status| status.current_fragment = fragment.clone());
        let stats = process_fragment_gc(&fragment, retention).await?;
        update_status(|status| {
            status.total_fragments_scanned = status.total_fragments_scanned.saturating_add(1);
            status.total_live_versions_seen = status
                .total_live_versions_seen
                .saturating_add(stats.live_objects_seen);
            status.total_trash_versions_expired = status
                .total_trash_versions_expired
                .saturating_add(stats.trash_versions_expired);
            status.total_sessions_deleted = status
                .total_sessions_deleted
                .saturating_add(stats.sessions_deleted);
            status.total_refund_bytes =
                status.total_refund_bytes.saturating_add(stats.refund_bytes);
            status.duration_seconds = started.elapsed().as_secs();
        });
    }
    update_status(|status| status.duration_seconds = started.elapsed().as_secs());
    Ok(())
}

fn begin_job(is_manual_run: bool) -> Result<(), AppError> {
    if GC_RUNNING.swap(true, Ordering::SeqCst) {
        return Err(AppError::Conflict("GC is already running".into()));
    }
    GC_STOP_SIGNAL.store(false, Ordering::Relaxed);
    update_status(|status| {
        *status = GcJobStatus {
            is_running: true,
            phase: "initializing".into(),
            is_manual_run,
            start_timestamp: Utc::now().timestamp(),
            ..GcJobStatus::default()
        };
    });
    Ok(())
}

fn finish_job(result: Result<(), AppError>) {
    match result {
        Ok(()) => update_status(|status| {
            status.is_running = false;
            if status.phase != "Stopped by User" {
                status.phase = "Completed".into();
            }
        }),
        Err(error) => {
            error!("N2 3.1 NCNX generation garbage collection failed: {error}");
            update_status(|status| {
                status.is_running = false;
                status.phase = "Failed".into();
                status.total_errors = status.total_errors.saturating_add(1);
                status.last_error = Some(error.to_string());
            });
        }
    }
    GC_STOP_SIGNAL.store(false, Ordering::Relaxed);
    GC_RUNNING.store(false, Ordering::SeqCst);
}

pub async fn run_lifecycle_manager(_state: Arc<AppState>) {
    let interval = Duration::from_secs(CONFIG.garbage_collector.cleanup_interval_seconds.max(1));
    loop {
        sleep(interval).await;
        if begin_job(false).is_err() {
            continue;
        }
        finish_job(perform_gc_scan(false, None, GcRunOptions::default()).await);
    }
}

pub async fn start_garbage_collector_job(
    _state: Arc<AppState>,
    target_fragment: Option<String>,
    mode: Option<String>,
    fast_mode: Option<bool>,
) -> Result<(), AppError> {
    crate::core::operation_gate::ensure_client_mutation_allowed()?;
    let target_fragment = match target_fragment {
        Some(fragment) => Some(resolve_stable_fragment_id(&fragment).await?),
        None => None,
    };
    let mut params = HashMap::new();
    if let Some(mode) = mode {
        params.insert("mode".to_string(), mode);
    }
    let options = GcRunOptions {
        execution_mode: parse_gc_execution_mode(&params),
        fast_mode: fast_mode.unwrap_or(false),
    };
    begin_job(true)?;
    tokio::spawn(async move {
        finish_job(perform_gc_scan(true, target_fragment, options).await);
    });
    Ok(())
}

pub async fn run_garbage_collector(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let target = params
        .get("fragment")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let mode = parse_gc_execution_mode(&params);
    let fast = parse_bool_param(&params, "fast_mode", false);
    match start_garbage_collector_job(state, target, Some(mode.as_str().to_string()), Some(fast))
        .await
    {
        Ok(()) => (StatusCode::ACCEPTED, "GC started".to_string()),
        Err(AppError::Conflict(message)) => (StatusCode::CONFLICT, message),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
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
        return (StatusCode::OK, "GC is not running");
    }
    GC_STOP_SIGNAL.store(true, Ordering::Relaxed);
    update_status(|status| status.phase = "Stopping".into());
    (StatusCode::OK, "Stop signal sent")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gc_modes_are_explicit_and_fast_is_not_implicit() {
        let mut params = HashMap::new();
        assert_eq!(
            parse_gc_execution_mode(&params),
            GcExecutionMode::Availability
        );
        assert!(!parse_bool_param(&params, "fast_mode", false));
        params.insert("mode".into(), "strict".into());
        assert_eq!(parse_gc_execution_mode(&params), GcExecutionMode::Strict);
        params.insert("fast_mode".into(), "true".into());
        assert!(parse_bool_param(&params, "fast_mode", false));
    }

    #[test]
    fn ttl_age_is_saturating_for_future_timestamps() {
        let now = 100i64;
        let future = 200i64;
        let age = Duration::from_secs(now.saturating_sub(future.max(0)).max(0) as u64);
        assert_eq!(age, Duration::ZERO);
    }
}
