use crate::core::constants::FRAGMENT_METADATA_FILE;
use crate::core::disk_manager::ObjectPathManager;
use crate::core::fragment_identity::{
    list_fragment_aliases, physical_fragment_root_on, stable_fragment_id_for_root,
};
use crate::core::fragment_metadata::resolve_stable_fragment_id;
use crate::core::index;
use crate::core::operation_gate::{MaintenanceAccessMode, acquire_maintenance_mode};
use crate::distribution::ncnx::{
    key_references_object, list_canonical_names, ncnx_cursor, read_canonical_record, remove_trash,
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
use tracing::{error, info, warn};

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
    pub total_trash_reclamations_deferred: u64,
    pub total_sessions_deleted: u64,
    pub total_sessions_deferred: u64,
    pub total_orphan_blob_dirs_deleted: u64,
    pub total_refund_bytes: u64,
    pub total_errors: u64,
    pub total_deferred_runs: u64,
    pub current_fragment: String,
    pub last_error: Option<String>,
    pub last_deferred: Option<String>,
    pub start_timestamp: i64,
    pub duration_seconds: u64,
}

#[derive(Debug, Default)]
struct FragmentGcStats {
    live_objects_seen: u64,
    trash_versions_expired: u64,
    trash_reclamations_deferred: u64,
    sessions_deleted: u64,
    sessions_deferred: u64,
    refund_bytes: u64,
}

fn is_retryable_gc_unavailability(error: &AppError) -> bool {
    matches!(
        error,
        AppError::ServiceUnavailable(_) | AppError::Timeout(_)
    )
}

fn defer_if_namespace_changed(error: AppError, fragment: &str, phase: &str) -> AppError {
    match error {
        // Listing and opening an immutable NCNX are separate lockless reads.
        // Rename, trash, recover, or a peer GC may validly remove the listed
        // name between them. Restart the proof from page one: continuing from
        // the old cursor could miss the destination and reclaim a live object.
        AppError::NotFound => AppError::ServiceUnavailable(format!(
            "namespace changed during GC {phase} for fragment '{fragment}'; retry from the beginning"
        )),
        error => error,
    }
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
            .await
            .map_err(|error| defer_if_namespace_changed(error, fragment, "live-reference scan"))?;
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
                match read_canonical_record(fragment, &name).await {
                    Ok(located) => {
                        protected.insert(located.record.object_id);
                    }
                    Err(AppError::NotFound) => {
                        return Err(defer_if_namespace_changed(
                            AppError::NotFound,
                            fragment,
                            "unexpired-trash scan",
                        ));
                    }
                    Err(error) => return Err(error),
                }
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
            let located = match read_canonical_record(fragment, &name).await {
                Ok(located) => located,
                Err(AppError::NotFound) => {
                    return Err(defer_if_namespace_changed(
                        AppError::NotFound,
                        fragment,
                        "expired-trash scan",
                    ));
                }
                Err(error) => return Err(error),
            };
            let object_id = located.record.object_id.clone();

            // Any live key or any still-recoverable trash key protects the
            // immutable object. Only this expired pointer is then discarded.
            if protected.contains(&object_id)
                || key_references_object(fragment, &name.logical_key, &object_id)
                    .await
                    .map_err(|error| {
                        defer_if_namespace_changed(error, fragment, "reference revalidation")
                    })?
            {
                remove_trash(&located, trash).await.map_err(|error| {
                    defer_if_namespace_changed(error, fragment, "trash-pointer removal")
                })?;
                continue;
            }

            defer_unproven_expired_reclamation(stats);
        }
        if !full {
            break;
        }
    }
    Ok(())
}

fn defer_unproven_expired_reclamation(stats: &mut FragmentGcStats) {
    // The earlier reference scan is not exclusive object ownership. Recovery,
    // copy or publication can create a reference after it and before a peer
    // purge; owning only the final trash-pointer removal cannot fence that.
    // Until all reference creators and holder-side purge share a proven object
    // lifecycle protocol, preserve both the last trash pointer and its payload.
    // A completed GC pass must not claim reclaimed bytes or expired versions.
    stats.trash_reclamations_deferred = stats.trash_reclamations_deferred.saturating_add(1);
}

async fn expire_upload_sessions(
    fragment: &str,
    now_unix: i64,
    stats: &mut FragmentGcStats,
) -> Result<(), AppError> {
    let ttl = i64::try_from(CONFIG.garbage_collector.file_ttl_seconds).unwrap_or(i64::MAX);
    if let Some(runtime) = crate::distribution::membership::try_active_cluster_runtime() {
        reclaim_owned_uploads(&runtime, fragment, now_unix, ttl, stats).await
    } else {
        inspect_expired_upload_sessions(fragment, now_unix, ttl, stats).await
    }
}

async fn reclaim_owned_uploads(
    runtime: &crate::distribution::membership::ClusterRuntime,
    fragment: &str,
    now_unix: i64,
    ttl: i64,
    stats: &mut FragmentGcStats,
) -> Result<(), AppError> {
    use crate::distribution::upload_lifecycle::{self, UploadLifecycleState};
    let mut inventory = upload_lifecycle::UploadInventory::new(runtime, fragment)?;
    loop {
        if should_stop_gc() {
            return Ok(());
        }
        let page = inventory.next_page(128).await?;
        for candidate in page.records {
            if should_stop_gc() {
                return Ok(());
            }
            if candidate.state == UploadLifecycleState::Committed
                || now_unix.saturating_sub(candidate.updated_at_unix.max(0)) <= ttl
            {
                continue;
            }
            let current = upload_lifecycle::inspect_upload(runtime, &candidate.identity).await?;
            let Some(current) = current else {
                stats.sessions_deferred = stats.sessions_deferred.saturating_add(1);
                continue;
            };
            if current.state == UploadLifecycleState::Committed
                || now_unix.saturating_sub(current.updated_at_unix.max(0)) <= ttl
            {
                continue;
            }
            match crate::distribution::upload_ownership::abort_and_reclaim_if_idle(
                runtime,
                &current.identity,
                Some(now_unix.saturating_sub(ttl)),
            )
            .await
            {
                Ok(()) => {
                    // Exact peer cleanup has acknowledged completion. Preserve
                    // the terminal ownership fence while retiring only this
                    // coordinator's known manifest/part/binding records.
                    index::remove_session(
                        fragment,
                        &current.identity.version_id,
                        "gc-certified-abort",
                    )
                    .await?;
                    stats.sessions_deleted = stats.sessions_deleted.saturating_add(1);
                }
                Err(error) => {
                    // A partial abort, committed/headless peer or unavailable
                    // holder cannot authorize guessing the missing evidence.
                    // Durable votes and upload records remain retryable.
                    warn!(
                        fragment,
                        version = current.identity.version_id,
                        "upload reclamation deferred: {error}"
                    );
                    stats.sessions_deferred = stats.sessions_deferred.saturating_add(1);
                }
            }
        }
        if page.finished {
            break;
        }
    }
    Ok(())
}

async fn inspect_expired_upload_sessions(
    fragment: &str,
    now_unix: i64,
    ttl: i64,
    stats: &mut FragmentGcStats,
) -> Result<(), AppError> {
    for session in index::list_active_sessions(fragment).await? {
        if should_stop_gc() {
            break;
        }
        if now_unix.saturating_sub(session_last_activity(&session)) <= ttl {
            continue;
        }
        // A session may retain a pending logical binding, receipts for peer
        // shards, or a committed NCDX awaiting NCNX publication. Age is not
        // upload-abort authority. Even an empty manifest can have an in-flight
        // part/finalizer, so deleting it would race those writers and lose the
        // only durable ownership/retry evidence. Retain every expired session
        // until holder-side lifecycle fencing can prove complete reclamation.
        stats.sessions_deferred = stats.sessions_deferred.saturating_add(1);
    }
    Ok(())
}

fn session_last_activity(session: &index::IndexSessionState) -> i64 {
    // SessionRow.last_activity_at was historically written only at init.
    // Existing PartMetadata carries completion time; use available state
    // timestamps without adding a metadata write to the upload hot path.
    // Compact filename receipts decode with the manifest time, so they are
    // not evidence of recent activity (nor authority to reclaim anything).
    std::iter::once(session.last_activity_at)
        .chain(std::iter::once(session.manifest.created_at))
        .chain(session.parts.values().map(|part| part.created_at))
        .chain(session.part_receipts.values().map(|part| part.created_at))
        .max()
        .unwrap_or(0)
        .max(0)
}

async fn process_fragment_gc(
    fragment: &str,
    retention: Duration,
) -> Result<FragmentGcStats, AppError> {
    if crate::core::export::pins::fragment_is_pinned(fragment).await? {
        tracing::info!(
            fragment,
            "deferring fragment GC while export retains source versions"
        );
        return Ok(FragmentGcStats::default());
    }
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
        "N2 3.4 NCNX generation garbage collection started"
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
            status.total_trash_reclamations_deferred = status
                .total_trash_reclamations_deferred
                .saturating_add(stats.trash_reclamations_deferred);
            status.total_sessions_deleted = status
                .total_sessions_deleted
                .saturating_add(stats.sessions_deleted);
            status.total_sessions_deferred = status
                .total_sessions_deferred
                .saturating_add(stats.sessions_deferred);
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
        Err(error) if is_retryable_gc_unavailability(&error) => {
            // GC must fail closed whenever the complete live/protected set
            // cannot be proven. A transient node outage therefore defers the
            // scan without classifying intact data as a GC failure. The next
            // independent node-local cycle retries from durable NCNX/NCDX.
            warn!("N2 3.4 NCNX generation garbage collection deferred: {error}");
            update_status(|status| {
                status.is_running = false;
                status.phase = "Deferred".into();
                status.total_deferred_runs = status.total_deferred_runs.saturating_add(1);
                status.last_deferred = Some(error.to_string());
            });
        }
        Err(error) => {
            error!("N2 3.4 NCNX generation garbage collection failed: {error}");
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

    #[test]
    fn only_transient_topology_failures_defer_gc() {
        assert!(is_retryable_gc_unavailability(
            &AppError::ServiceUnavailable("peer offline".into())
        ));
        assert!(is_retryable_gc_unavailability(&AppError::Timeout(
            "peer read".into()
        )));
        assert!(!is_retryable_gc_unavailability(&AppError::NotFound));
        assert!(!is_retryable_gc_unavailability(&AppError::Conflict(
            "replicas disagree".into()
        )));

        let deferred =
            defer_if_namespace_changed(AppError::NotFound, "fragment-a", "live-reference scan");
        assert!(is_retryable_gc_unavailability(&deferred));
        assert!(deferred.to_string().contains("retry from the beginning"));
    }
}

#[cfg(test)]
#[path = "garbage_collector/tests.rs"]
mod upload_expiry_tests;
