use crate::enums::app_error_enum::AppError;
use axum::body::Body;
use axum::http::Method;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{LazyLock, RwLock};

static READ_ONLY_FAST_MODE_REFCOUNT: LazyLock<AtomicUsize> = LazyLock::new(|| AtomicUsize::new(0));
static QUIESCED_MAINTENANCE_REFCOUNT: LazyLock<AtomicUsize> = LazyLock::new(|| AtomicUsize::new(0));
static TOPOLOGY_READ_ONLY_REASON: LazyLock<RwLock<Option<String>>> =
    LazyLock::new(|| RwLock::new(None));
static AUTO_HEALING_FAULT_REASON: LazyLock<RwLock<Option<String>>> =
    LazyLock::new(|| RwLock::new(None));

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
    } else if READ_ONLY_FAST_MODE_REFCOUNT.load(Ordering::Acquire) > 0
        || safety_read_only_reason().is_some()
    {
        MaintenanceAccessMode::ReadOnly
    } else {
        MaintenanceAccessMode::Availability
    }
}

pub fn set_safety_read_only(reason: Option<String>) {
    let mut current = TOPOLOGY_READ_ONLY_REASON
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *current = reason;
}

pub fn set_auto_healing_fault(reason: Option<String>) {
    let mut current = AUTO_HEALING_FAULT_REASON
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *current = reason;
}

pub fn safety_read_only_reason() -> Option<String> {
    let auto_healing = AUTO_HEALING_FAULT_REASON
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    auto_healing.or_else(|| {
        TOPOLOGY_READ_ONLY_REASON
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    })
}

pub fn topology_read_only_reason() -> Option<String> {
    TOPOLOGY_READ_ONLY_REASON
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

pub fn ensure_user_mutations_allowed() -> Result<(), AppError> {
    if current_maintenance_access_mode() >= MaintenanceAccessMode::ReadOnly {
        return Err(AppError::ServiceUnavailable(
            safety_read_only_reason().unwrap_or_else(|| {
                "N1 is read-only while strict maintenance is active.".to_string()
            }),
        ));
    }
    Ok(())
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

    match mode {
        MaintenanceAccessMode::Availability => next.run(req).await,
        MaintenanceAccessMode::ReadOnly => match safety_read_only_reason() {
            Some(reason) => (axum::http::StatusCode::SERVICE_UNAVAILABLE, reason).into_response(),
            None => (
                axum::http::StatusCode::FORBIDDEN,
                "Write/modify/delete operations are temporarily disabled: strict maintenance mode is active.",
            )
                .into_response(),
        },
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
        safety_read_only_reason, set_safety_read_only,
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

    #[test]
    fn read_only_fast_mode_guard_refcount_works() {
        let _test_lock = acquire_test_lock();
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
        assert!(!is_read_only_fast_mode_active());
        let g = acquire_read_only_fast_mode(false);
        assert!(!is_read_only_fast_mode_active());
        drop(g);
        assert!(!is_read_only_fast_mode_active());
    }

    #[test]
    fn persistent_safety_gate_is_independent_of_temporary_guards() {
        let _test_lock = acquire_test_lock();
        set_safety_read_only(Some("topology cannot satisfy 4D+2P".to_string()));
        assert_eq!(
            current_maintenance_access_mode(),
            MaintenanceAccessMode::ReadOnly
        );
        assert_eq!(
            safety_read_only_reason().as_deref(),
            Some("topology cannot satisfy 4D+2P")
        );

        let temporary = acquire_read_only_fast_mode(true);
        set_safety_read_only(None);
        assert_eq!(
            current_maintenance_access_mode(),
            MaintenanceAccessMode::ReadOnly
        );
        drop(temporary);
        assert_eq!(
            current_maintenance_access_mode(),
            MaintenanceAccessMode::Availability
        );
    }

    #[tokio::test]
    async fn middleware_blocks_mutations_but_allows_reads() {
        let _test_lock = acquire_test_lock();
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
    async fn persistent_safety_read_only_returns_service_unavailable() {
        let _test_lock = acquire_test_lock();
        let app = Router::new()
            .route("/write", post(|| async { "ok" }))
            .layer(from_fn(enforce_read_only_fast_mode_on_mutations));

        set_safety_read_only(Some("registered topology is unsafe".to_string()));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/write")
                    .body(Body::empty())
                    .expect("build write request"),
            )
            .await
            .expect("write request");
        set_safety_read_only(None);

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn quiesced_mode_blocks_reads_and_mutations() {
        let _test_lock = acquire_test_lock();
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
