use axum::body::Body;
use axum::http::Method;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{OnceLock, RwLock};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use ulid::Ulid;

use crate::core::disk_manager::FS_STORAGE_ROOT;
use crate::enums::app_error_enum::AppError;

static READ_ONLY_FAST_MODE_REFCOUNT: LazyLock<AtomicUsize> = LazyLock::new(|| AtomicUsize::new(0));
static QUIESCED_MAINTENANCE_REFCOUNT: LazyLock<AtomicUsize> = LazyLock::new(|| AtomicUsize::new(0));
static SAFETY_READ_ONLY: OnceLock<RwLock<Option<SafetyReadOnlyState>>> = OnceLock::new();

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SafetyReadOnlyState {
    pub incident_id: String,
    pub cluster_id: String,
    pub membership_epoch: u64,
    pub detecting_node_id: String,
    pub fragment_id: String,
    pub finding: String,
    pub report_filename: String,
    pub reason: String,
    pub activated_at_unix_nanos: i64,
}

fn safety_state() -> &'static RwLock<Option<SafetyReadOnlyState>> {
    SAFETY_READ_ONLY.get_or_init(|| RwLock::new(None))
}

fn safety_marker_path() -> Result<PathBuf, AppError> {
    FS_STORAGE_ROOT
        .get()
        .map(|root| {
            PathBuf::from(root)
                .join(".n2")
                .join("safety")
                .join("read-only.json")
        })
        .ok_or_else(|| AppError::InternalServerError("N2 storage root is not initialized".into()))
}

pub fn cluster_safety_read_only_state() -> Option<SafetyReadOnlyState> {
    safety_state().read().ok().and_then(|guard| guard.clone())
}

pub fn is_cluster_safety_read_only() -> bool {
    cluster_safety_read_only_state().is_some()
}

pub fn ensure_client_mutation_allowed() -> Result<(), AppError> {
    if let Some(state) = cluster_safety_read_only_state() {
        return Err(AppError::ServiceUnavailable(format!(
            "N2 cluster is safety read-only after '{}': {} (incident {})",
            state.finding, state.reason, state.incident_id
        )));
    }
    Ok(())
}

/// Load the node-local copy of the cluster safety latch before request
/// handling starts. The marker is operational state, not namespace data.
pub fn load_safety_read_only_marker() -> Result<Option<SafetyReadOnlyState>, AppError> {
    let path = safety_marker_path()?;
    let state = match std::fs::read(&path) {
        Ok(bytes) => Some(
            serde_json::from_slice::<SafetyReadOnlyState>(&bytes).map_err(|error| {
                AppError::InternalServerError(format!(
                    "decode N2 safety marker {}: {error}",
                    path.display()
                ))
            })?,
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(AppError::Io(error, format!("read {}", path.display()))),
    };
    if let Ok(mut guard) = safety_state().write() {
        *guard = state.clone();
    }
    Ok(state)
}

/// Persist an idempotent node-local copy of a cluster read-only incident.
/// Reports remain local and are not carried in this marker.
pub async fn apply_safety_read_only_state(state: SafetyReadOnlyState) -> Result<(), AppError> {
    if cluster_safety_read_only_state().is_some() {
        return Ok(());
    }
    let path = safety_marker_path()?;
    let parent = path
        .parent()
        .ok_or_else(|| AppError::InternalServerError("N2 safety marker has no parent".into()))?;
    fs::create_dir_all(parent)
        .await
        .map_err(|error| AppError::Io(error, format!("create {}", parent.display())))?;
    let temporary = parent.join(format!(".read-only-{}.tmp", Ulid::new()));
    let bytes = serde_json::to_vec_pretty(&state).map_err(|error| {
        AppError::InternalServerError(format!("encode N2 safety marker: {error}"))
    })?;
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .await
        .map_err(|error| AppError::Io(error, format!("create {}", temporary.display())))?;
    file.write_all(&bytes)
        .await
        .map_err(|error| AppError::Io(error, format!("write {}", temporary.display())))?;
    file.sync_all()
        .await
        .map_err(|error| AppError::Io(error, format!("sync {}", temporary.display())))?;
    drop(file);
    fs::rename(&temporary, &path)
        .await
        .map_err(|error| AppError::Io(error, format!("publish {}", path.display())))?;
    let directory = fs::File::open(parent)
        .await
        .map_err(|error| AppError::Io(error, format!("open {}", parent.display())))?;
    directory
        .sync_all()
        .await
        .map_err(|error| AppError::Io(error, format!("sync {}", parent.display())))?;
    if let Ok(mut guard) = safety_state().write() {
        *guard = Some(state);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum MaintenanceAccessMode {
    #[default]
    Availability,
    ReadOnly,
    Quiesced,
}

pub struct ReadOnlyFastModeGuard {
    mode: MaintenanceAccessMode,
    active: bool,
}

impl Drop for ReadOnlyFastModeGuard {
    fn drop(&mut self) {
        if self.active {
            match self.mode {
                MaintenanceAccessMode::Availability => {}
                MaintenanceAccessMode::ReadOnly => {
                    READ_ONLY_FAST_MODE_REFCOUNT.fetch_sub(1, Ordering::AcqRel);
                }
                MaintenanceAccessMode::Quiesced => {
                    QUIESCED_MAINTENANCE_REFCOUNT.fetch_sub(1, Ordering::AcqRel);
                }
            }
        }
    }
}

pub fn acquire_maintenance_mode(mode: MaintenanceAccessMode) -> ReadOnlyFastModeGuard {
    match mode {
        MaintenanceAccessMode::Availability => ReadOnlyFastModeGuard {
            mode,
            active: false,
        },
        MaintenanceAccessMode::ReadOnly => {
            READ_ONLY_FAST_MODE_REFCOUNT.fetch_add(1, Ordering::AcqRel);
            ReadOnlyFastModeGuard { mode, active: true }
        }
        MaintenanceAccessMode::Quiesced => {
            QUIESCED_MAINTENANCE_REFCOUNT.fetch_add(1, Ordering::AcqRel);
            ReadOnlyFastModeGuard { mode, active: true }
        }
    }
}

#[allow(dead_code)]
pub fn acquire_read_only_fast_mode(enable: bool) -> ReadOnlyFastModeGuard {
    if enable {
        acquire_maintenance_mode(MaintenanceAccessMode::ReadOnly)
    } else {
        acquire_maintenance_mode(MaintenanceAccessMode::Availability)
    }
}

pub fn current_maintenance_access_mode() -> MaintenanceAccessMode {
    if QUIESCED_MAINTENANCE_REFCOUNT.load(Ordering::Acquire) > 0 {
        MaintenanceAccessMode::Quiesced
    } else if READ_ONLY_FAST_MODE_REFCOUNT.load(Ordering::Acquire) > 0 {
        MaintenanceAccessMode::ReadOnly
    } else {
        MaintenanceAccessMode::Availability
    }
}

#[allow(dead_code)]
pub fn is_read_only_fast_mode_active() -> bool {
    current_maintenance_access_mode() >= MaintenanceAccessMode::ReadOnly
}

pub async fn enforce_read_only_fast_mode_on_mutations(req: Request<Body>, next: Next) -> Response {
    if matches!(req.method(), &Method::OPTIONS) {
        return next.run(req).await;
    }

    let mode = current_maintenance_access_mode();
    if matches!(req.method(), &Method::GET | &Method::HEAD) {
        if mode == MaintenanceAccessMode::Quiesced {
            return (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "Read operations are temporarily disabled: scrub mode is active.",
            )
                .into_response();
        }
        return next.run(req).await;
    }

    if let Err(error) = ensure_client_mutation_allowed() {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            error.to_string(),
        )
            .into_response();
    }

    match mode {
        MaintenanceAccessMode::Availability => next.run(req).await,
        MaintenanceAccessMode::ReadOnly => (
            axum::http::StatusCode::FORBIDDEN,
            "Write/modify/delete operations are temporarily disabled: strict maintenance mode is active.",
        )
            .into_response(),
        MaintenanceAccessMode::Quiesced => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "Write/modify/delete operations are temporarily disabled: scrub mode is active.",
        )
            .into_response(),
    }
}

pub async fn enforce_quiesced_mode_on_reads(req: Request<Body>, next: Next) -> Response {
    if matches!(req.method(), &Method::OPTIONS) {
        return next.run(req).await;
    }

    if current_maintenance_access_mode() == MaintenanceAccessMode::Quiesced {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "Read operations are temporarily disabled: scrub mode is active.",
        )
            .into_response();
    }

    next.run(req).await
}

#[cfg(test)]
mod tests {
    use super::{
        MaintenanceAccessMode, acquire_maintenance_mode, acquire_read_only_fast_mode,
        current_maintenance_access_mode, enforce_quiesced_mode_on_reads,
        enforce_read_only_fast_mode_on_mutations, is_read_only_fast_mode_active,
    };
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::middleware::from_fn;
    use axum::routing::{get, post};
    use std::sync::atomic::{AtomicBool, Ordering};
    use tower::ServiceExt;

    static TEST_LOCK: AtomicBool = AtomicBool::new(false);

    struct TestLockGuard;

    impl Drop for TestLockGuard {
        fn drop(&mut self) {
            TEST_LOCK.store(false, Ordering::Release);
        }
    }

    fn acquire_test_lock() -> TestLockGuard {
        while TEST_LOCK
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            std::thread::yield_now();
        }
        TestLockGuard
    }

    fn clear_safety_state() {
        if let Ok(mut guard) = super::safety_state().write() {
            *guard = None;
        }
    }

    #[test]
    fn read_only_fast_mode_guard_refcount_works() {
        let _test_lock = acquire_test_lock();
        clear_safety_state();
        assert!(!is_read_only_fast_mode_active());
        let g1 = acquire_read_only_fast_mode(true);
        assert!(is_read_only_fast_mode_active());
        let g2 = acquire_read_only_fast_mode(true);
        assert!(is_read_only_fast_mode_active());
        drop(g2);
        assert!(is_read_only_fast_mode_active());
        drop(g1);
        assert!(!is_read_only_fast_mode_active());
    }

    #[test]
    fn disabled_guard_is_noop() {
        let _test_lock = acquire_test_lock();
        clear_safety_state();
        assert!(!is_read_only_fast_mode_active());
        let g = acquire_read_only_fast_mode(false);
        assert!(!is_read_only_fast_mode_active());
        drop(g);
        assert!(!is_read_only_fast_mode_active());
    }

    #[tokio::test]
    async fn middleware_blocks_mutations_but_allows_reads() {
        let _test_lock = acquire_test_lock();
        clear_safety_state();
        let app = Router::new()
            .route("/read", get(|| async { "ok" }))
            .route("/write", post(|| async { "ok" }))
            .layer(from_fn(enforce_read_only_fast_mode_on_mutations));

        let guard = acquire_read_only_fast_mode(true);

        let write_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/write")
                    .body(Body::empty())
                    .expect("build write request"),
            )
            .await
            .expect("write request");
        assert_eq!(write_resp.status(), StatusCode::FORBIDDEN);

        let read_resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/read")
                    .body(Body::empty())
                    .expect("build read request"),
            )
            .await
            .expect("read request");
        assert_eq!(read_resp.status(), StatusCode::OK);

        drop(guard);
    }

    #[tokio::test]
    async fn quiesced_mode_blocks_reads_and_mutations() {
        let _test_lock = acquire_test_lock();
        clear_safety_state();
        let app = Router::new()
            .route("/read", get(|| async { "ok" }))
            .route("/write", post(|| async { "ok" }))
            .layer(from_fn(enforce_read_only_fast_mode_on_mutations));

        let guard = acquire_maintenance_mode(MaintenanceAccessMode::Quiesced);
        assert_eq!(
            current_maintenance_access_mode(),
            MaintenanceAccessMode::Quiesced
        );

        let read_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/read")
                    .body(Body::empty())
                    .expect("build read request"),
            )
            .await
            .expect("read request");
        assert_eq!(read_resp.status(), StatusCode::SERVICE_UNAVAILABLE);

        let write_resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/write")
                    .body(Body::empty())
                    .expect("build write request"),
            )
            .await
            .expect("write request");
        assert_eq!(write_resp.status(), StatusCode::SERVICE_UNAVAILABLE);

        drop(guard);
    }

    #[tokio::test]
    async fn quiesced_read_middleware_blocks_all_read_route_methods() {
        let _test_lock = acquire_test_lock();
        clear_safety_state();
        let app = Router::new()
            .route("/read-get", get(|| async { "ok" }))
            .route("/read-post", post(|| async { "ok" }))
            .layer(from_fn(enforce_quiesced_mode_on_reads));

        let guard = acquire_maintenance_mode(MaintenanceAccessMode::Quiesced);

        let get_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/read-get")
                    .body(Body::empty())
                    .expect("build get request"),
            )
            .await
            .expect("get request");
        assert_eq!(get_resp.status(), StatusCode::SERVICE_UNAVAILABLE);

        let post_resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/read-post")
                    .body(Body::empty())
                    .expect("build post request"),
            )
            .await
            .expect("post request");
        assert_eq!(post_resp.status(), StatusCode::SERVICE_UNAVAILABLE);

        drop(guard);
    }
}
