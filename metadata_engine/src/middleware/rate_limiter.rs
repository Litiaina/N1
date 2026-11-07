//! M1 owns its admission policy. Both named transports share the same buckets;
//! caller-supplied forwarding headers never choose the peer identity.
use crate::server::{
    App,
    error::{ApiError, ApiResult},
};
use axum::{
    extract::{ConnectInfo, Request, State},
    http::{Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};
use n1_link::PoolId;
use std::{
    net::{IpAddr, SocketAddr},
    num::NonZeroU32,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub(crate) struct RateLimits {
    pub enabled: bool,
    auth: DefaultKeyedRateLimiter<IpAddr>,
    global: DefaultKeyedRateLimiter<IpAddr>,
    view: DefaultKeyedRateLimiter<PoolId>,
    upload: DefaultKeyedRateLimiter<PoolId>,
    stop: CancellationToken,
    pruner: Mutex<Option<JoinHandle<()>>>,
}

fn quota(rate: u64, burst: u32) -> Quota {
    // The INI parser checks nonzero rates/bursts and the nanosecond bound.
    // Round up, matching N1: quantization must never increase the operator's
    // configured rate (not all reciprocals are whole nanoseconds).
    Quota::with_period(Duration::from_nanos(1_000_000_000_u64.div_ceil(rate)))
        .expect("validated request rate")
        .allow_burst(NonZeroU32::new(burst).expect("validated request burst"))
}

fn limited() -> ApiError {
    ApiError(
        StatusCode::TOO_MANY_REQUESTS,
        "Request rate limit exceeded".into(),
    )
}

impl RateLimits {
    pub(crate) fn new(settings: &cli_core::settings::RateLimits) -> Self {
        let configured_quota = |rate, burst| {
            if settings.enable_rate_limiting {
                quota(rate, burst)
            } else {
                quota(1, 1)
            }
        };
        Self {
            enabled: settings.enable_rate_limiting,
            auth: RateLimiter::keyed(configured_quota(
                settings.auth_limit_per_second,
                settings.auth_limit_burst_size,
            )),
            global: RateLimiter::keyed(configured_quota(
                settings.global_limit_per_second,
                settings.global_limit_burst_size,
            )),
            view: RateLimiter::keyed(configured_quota(
                settings.view_limit_per_second,
                settings.view_limit_burst_size,
            )),
            upload: RateLimiter::keyed(configured_quota(
                settings.upload_limit_per_second,
                settings.upload_limit_burst_size,
            )),
            stop: CancellationToken::new(),
            pruner: Mutex::new(None),
        }
    }

    /// Cleanup never runs in a request or on an async I/O worker. A weak owner
    /// lets an abandoned App drop; normal shutdown cancels and drains the task.
    pub(crate) fn start(self: &Arc<Self>) {
        if !self.enabled {
            return;
        }
        let mut task = self.pruner.lock().unwrap_or_else(|e| e.into_inner());
        if task.is_some() {
            return;
        }
        let owner = Arc::downgrade(self);
        let stop = self.stop.clone();
        *task = Some(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = stop.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_secs(60)) => (),
                }
                let Some(limits) = owner.upgrade() else {
                    return;
                };
                if tokio::task::spawn_blocking(move || {
                    // Only completely replenished buckets can be retired.
                    limits.auth.retain_recent();
                    limits.global.retain_recent();
                    limits.view.retain_recent();
                    limits.upload.retain_recent();
                })
                .await
                .is_err()
                {
                    return;
                }
            }
        }));
    }

    pub(crate) async fn shutdown(&self) {
        self.stop.cancel();
        let task = self.pruner.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }

    pub(crate) fn peer(&self, peer: IpAddr, authentication: bool) -> ApiResult<()> {
        if !self.enabled {
            return Ok(());
        }
        self.global.check_key(&peer).map_err(|_| limited())?;
        if authentication {
            self.auth.check_key(&peer).map_err(|_| limited())?;
        }
        Ok(())
    }

    pub(crate) fn operation(&self, fragment: &str, upload: bool) -> ApiResult<()> {
        if !self.enabled {
            return Ok(());
        }
        let limiter = if upload { &self.upload } else { &self.view };
        let identity = ulid::Ulid::from_string(fragment).map_err(|_| ApiError::unauthorized())?;
        limiter
            .check_key(&PoolId(identity.to_bytes()))
            .map_err(|_| limited())
    }
}

impl Drop for RateLimits {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

/// Native preflight charged the physical peer and operation before accepting a
/// raw body. This extension is supplied by server code, never a wire header.
#[derive(Clone, Copy)]
pub(crate) struct NativeAdmitted;

pub(crate) async fn admit(State(app): State<Arc<App>>, request: Request, next: Next) -> Response {
    if !app.rate_limits.enabled
        || request.method() == Method::OPTIONS
        || request.extensions().get::<NativeAdmitted>().is_some()
    {
        return next.run(request).await;
    }
    let result = (|| {
        {
            let peer = request
                .extensions()
                .get::<ConnectInfo<SocketAddr>>()
                .map(|peer| peer.0.ip())
                .ok_or_else(|| {
                    ApiError(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "Request peer identity unavailable".into(),
                    )
                })?;
            let path = request.uri().path();
            let authentication = path.starts_with("/noa/v1/auth/")
                || path == "/noa/v1/fragments" && request.method() == Method::POST;
            app.rate_limits.peer(peer, authentication)?;
        }
        // Verify the signature before choosing a fragment bucket. Handlers
        // still perform authoritative epoch/permissions checks. A token refresh
        // or a second transport cannot obtain a fresh per-fragment quota.
        if let Ok(claims) = super::auth::decode_claims(&app, request.headers()) {
            app.rate_limits.operation(
                &claims.fragment,
                request.method() != Method::GET
                    && request.uri().path().starts_with("/noa/v1/upload/"),
            )?;
        }
        Ok::<(), ApiError>(())
    })();
    match result {
        Ok(()) => next.run(request).await,
        Err(error) => {
            let mut response = error.into_response();
            if response.status() == StatusCode::TOO_MANY_REQUESTS {
                response
                    .headers_mut()
                    .insert("retry-after", "1".parse().expect("static header"));
            }
            response
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const FRAGMENT: &str = "01M6TZ30FSRC9P2HX3G22CXYDR";
    const OTHER_FRAGMENT: &str = "01M6TZ30FSRC9P2HX3G22CXYDS";
    #[test]
    fn fractional_period_never_increases_the_configured_rate() {
        assert_eq!(
            quota(3, 1).replenish_interval(),
            Duration::from_nanos(333333334)
        );
        assert_eq!(
            quota(600000000, 1).replenish_interval(),
            Duration::from_nanos(2)
        );
        assert_eq!(
            quota(10000, 1).replenish_interval(),
            Duration::from_nanos(100000)
        );
    }
    fn limits(enabled: bool) -> RateLimits {
        RateLimits::new(&cli_core::settings::RateLimits {
            enable_rate_limiting: enabled,
            auth_limit_per_second: 1,
            auth_limit_burst_size: 2,
            global_limit_per_second: 1,
            global_limit_burst_size: 10,
            view_limit_per_second: 1,
            view_limit_burst_size: 3,
            upload_limit_per_second: 1,
            upload_limit_burst_size: 4,
        })
    }
    #[test]
    fn buckets_separate_auth_upload_view_and_physical_peers() {
        let limits = limits(true);
        let peer = "127.0.0.1".parse().unwrap();
        for _ in 0..2 {
            limits.peer(peer, true).unwrap();
        }
        assert_eq!(
            limits.peer(peer, true).unwrap_err().0,
            StatusCode::TOO_MANY_REQUESTS
        );
        limits.peer("127.0.0.2".parse().unwrap(), true).unwrap();
        for _ in 0..3 {
            limits.operation(FRAGMENT, false).unwrap();
        }
        assert!(limits.operation(FRAGMENT, false).is_err());
        for _ in 0..4 {
            limits.operation(FRAGMENT, true).unwrap();
        }
        assert!(limits.operation(FRAGMENT, true).is_err());
        limits.operation(OTHER_FRAGMENT, false).unwrap();
    }
    #[test]
    fn simultaneous_connections_share_one_fragment_burst() {
        let limits = Arc::new(limits(true));
        let barrier = Arc::new(std::sync::Barrier::new(16));
        let workers: Vec<_> = (0..16)
            .map(|_| {
                let limits = limits.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    limits.operation(FRAGMENT, true).is_ok()
                })
            })
            .collect();
        assert_eq!(
            workers
                .into_iter()
                .map(|w| w.join().unwrap())
                .filter(|ok| *ok)
                .count(),
            4
        );
    }
    #[test]
    fn disabled_policy_does_not_allocate_buckets() {
        let limits = limits(false);
        for _ in 0..10000 {
            limits.peer("127.0.0.1".parse().unwrap(), true).unwrap();
            limits.operation("fragment", true).unwrap();
        }
        assert_eq!(
            limits.global.len() + limits.auth.len() + limits.upload.len(),
            0
        );
    }

    #[test]
    fn disabled_policy_accepts_zero_values_without_dividing_by_zero() {
        let settings = cli_core::settings::RateLimits {
            enable_rate_limiting: false,
            auth_limit_per_second: 0,
            auth_limit_burst_size: 0,
            global_limit_per_second: 0,
            global_limit_burst_size: 0,
            view_limit_per_second: 0,
            view_limit_burst_size: 0,
            upload_limit_per_second: 0,
            upload_limit_burst_size: 0,
        };
        let limits = RateLimits::new(&settings);
        limits.peer("127.0.0.1".parse().unwrap(), true).unwrap();
        limits.operation("fragment", true).unwrap();
    }

    #[tokio::test]
    async fn cleanup_is_single_owned_task_and_shutdown_drains_it() {
        let limits = Arc::new(limits(true));
        limits.start();
        let task_id = limits.pruner.lock().unwrap().as_ref().unwrap().id();
        limits.start();
        assert_eq!(
            limits.pruner.lock().unwrap().as_ref().unwrap().id(),
            task_id
        );
        limits.operation(FRAGMENT, true).unwrap();
        limits.shutdown().await;
        assert!(limits.pruner.lock().unwrap().is_none());
        assert_eq!(limits.upload.len(), 1);
    }

    #[test]
    fn malformed_identity_cannot_allocate_fragment_buckets() {
        let limits = limits(true);
        assert_eq!(
            limits.operation("invalid", true).unwrap_err().0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(limits.upload.len(), 0);
    }

    #[tokio::test]
    #[ignore = "exercises the real 60-second cleanup period; opt-in lifecycle witness"]
    async fn background_cleanup_retires_idle_buckets_without_resetting_busy_quotas() {
        let limits = Arc::new(limits(true));
        limits.operation(OTHER_FRAGMENT, true).unwrap();
        limits.operation(FRAGMENT, true).unwrap();
        limits.start();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(70);
        let (mut accepted, mut rejected) = (0, 0);
        while limits.upload.len() != 1 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "idle bucket was not retired"
            );
            if limits.operation(FRAGMENT, true).is_ok() {
                accepted += 1;
            } else {
                rejected += 1;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(accepted >= 50 && rejected >= 50);
        // An active quota has not been replaced with a fresh four-token burst.
        assert!(
            (0..4)
                .filter(|_| limits.operation(FRAGMENT, true).is_ok())
                .count()
                <= 1
        );
        limits.shutdown().await;
    }
}
