use axum::body::Body;
use axum::http::Method;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicUsize, Ordering};

static READ_ONLY_FAST_MODE_REFCOUNT: LazyLock<AtomicUsize> = LazyLock::new(|| AtomicUsize::new(0));

pub struct ReadOnlyFastModeGuard {
    active: bool,
}

impl Drop for ReadOnlyFastModeGuard {
    fn drop(&mut self) {
        if self.active {
            READ_ONLY_FAST_MODE_REFCOUNT.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

pub fn acquire_read_only_fast_mode(enable: bool) -> ReadOnlyFastModeGuard {
    if enable {
        READ_ONLY_FAST_MODE_REFCOUNT.fetch_add(1, Ordering::AcqRel);
        ReadOnlyFastModeGuard { active: true }
    } else {
        ReadOnlyFastModeGuard { active: false }
    }
}

pub fn is_read_only_fast_mode_active() -> bool {
    READ_ONLY_FAST_MODE_REFCOUNT.load(Ordering::Acquire) > 0
}

pub async fn enforce_read_only_fast_mode_on_mutations(req: Request<Body>, next: Next) -> Response {
    if matches!(
        req.method(),
        &Method::GET | &Method::HEAD | &Method::OPTIONS
    ) {
        return next.run(req).await;
    }
    if is_read_only_fast_mode_active() {
        return (
            axum::http::StatusCode::FORBIDDEN,
            "Write/modify/delete operations are temporarily disabled: maintenance fast mode is active.",
        )
            .into_response();
    }
    next.run(req).await
}

#[cfg(test)]
mod tests {
    use super::{
        acquire_read_only_fast_mode, enforce_read_only_fast_mode_on_mutations,
        is_read_only_fast_mode_active,
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
}
