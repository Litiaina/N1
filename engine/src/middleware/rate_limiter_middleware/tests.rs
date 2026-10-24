use super::*;
use axum::extract::ConnectInfo;
use axum::http::StatusCode;
use axum::routing::get;
use governor::clock::FakeRelativeClock;
use governor::{Quota, RateLimiter};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroU32;
use tower::{Layer, ServiceExt};

// Inspect the quota produced by the actual middleware builder, not a parallel
// copy of the conversion formula. Batch exhaustion avoids timing short sleeps.
fn layer_quota<K: KeyExtractor>(
    layer: GovernorLayer<K, NoOpMiddleware, Body>,
    key: K::Key,
    burst: u32,
) -> Quota {
    let service = layer.layer(Router::<()>::new());
    let amount = NonZeroU32::new(burst).unwrap();
    assert!(service.limiter.check_key_n(&key, amount).unwrap().is_ok());
    service
        .limiter
        .check_key_n(&key, amount)
        .unwrap()
        .expect_err("the entire burst cannot replenish immediately")
        .quota()
}

fn quotas(settings: RateLimitSettings) -> [Quota; 4] {
    let layers = RateLimitLayers::from_settings(settings);
    let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
    [
        layer_quota(layers.auth.unwrap(), ip, settings.auth_burst),
        layer_quota(layers.view.unwrap(), "fragment".into(), settings.view_burst),
        layer_quota(
            layers.upload.unwrap(),
            "fragment".into(),
            settings.upload_burst,
        ),
        layer_quota(layers.global.unwrap(), ip, settings.global_burst),
    ]
}

#[test]
fn all_categories_use_requests_per_second_and_preserve_default_bursts() {
    let actual = quotas(RateLimitSettings::default());
    for (quota, period_ns, burst) in [
        (actual[0], 500_000_000, 5),
        (actual[1], 33_333_334, 100),
        (actual[2], 3_333_334, 3000),
        (actual[3], 5_000_000, 300),
    ] {
        assert_eq!(quota.replenish_interval(), Duration::from_nanos(period_ns));
        assert_eq!(quota.burst_size().get(), burst);
    }
}

#[test]
fn configured_rates_and_bursts_are_not_replaced_with_defaults() {
    let settings = RateLimitSettings {
        auth_per_sec: 4,
        auth_burst: 7,
        view_per_sec: 25,
        view_burst: 31,
        upload_per_sec: 125,
        upload_burst: 137,
        global_per_sec: 5000,
        global_burst: 10000,
        ..RateLimitSettings::default()
    };
    let actual = quotas(settings);
    for (quota, period_ns, burst) in [
        (actual[0], 250_000_000, 7),
        (actual[1], 40_000_000, 31),
        (actual[2], 8_000_000, 137),
        (actual[3], 200_000, 10000),
    ] {
        assert_eq!(quota.replenish_interval(), Duration::from_nanos(period_ns));
        assert_eq!(quota.burst_size().get(), burst);
    }
}

#[test]
fn exhausted_quotas_refill_one_token_per_interval_and_cap_at_the_burst() {
    for quota in quotas(RateLimitSettings::default()) {
        let clock = FakeRelativeClock::default();
        let limiter = RateLimiter::direct_with_clock(quota, clock.clone());
        let amount = quota.burst_size();
        assert!(limiter.check_n(amount).unwrap().is_ok());
        assert!(limiter.check().is_err());

        let period = quota.replenish_interval();
        clock.advance(period - Duration::from_nanos(1));
        assert!(limiter.check().is_err());
        clock.advance(Duration::from_nanos(1));
        assert!(limiter.check().is_ok());
        assert!(limiter.check().is_err());

        clock.advance(period * amount.get());
        assert!(limiter.check_n(amount).unwrap().is_ok());
        assert!(limiter.check().is_err());

        clock.advance(period * (amount.get() + 10));
        assert!(limiter.check_n(amount).unwrap().is_ok());
        assert!(
            limiter.check().is_err(),
            "idle time must not enlarge the burst"
        );
    }
}

#[test]
fn raising_the_configured_rate_makes_replenishment_faster() {
    let slow = layer_quota(
        GovernorLayer::new(governor_config(30, 100, UserKeyExtractor)),
        "fragment".into(),
        100,
    );
    let fast = layer_quota(
        GovernorLayer::new(governor_config(300, 100, UserKeyExtractor)),
        "fragment".into(),
        100,
    );
    assert!(fast.replenish_interval() < slow.replenish_interval());
}

#[test]
#[should_panic(expected = "requests per second must be between")]
fn zero_rate_is_rejected() {
    governor_config(0, 100, SmartIpKeyExtractor);
}

#[test]
#[should_panic(expected = "requests per second must be between")]
fn unrepresentable_rate_is_rejected() {
    governor_config(1_000_000_001, 100, SmartIpKeyExtractor);
}

#[test]
#[should_panic(expected = "burst size must be nonzero")]
fn zero_burst_is_rejected() {
    governor_config(30, 0, SmartIpKeyExtractor);
}

#[test]
fn nanosecond_precision_limit_is_supported() {
    governor_config(1_000_000_000, 1, SmartIpKeyExtractor);
}

#[tokio::test]
async fn disabled_limits_bypass_all_categories_even_with_zero_values() {
    let layers = RateLimitLayers::from_settings(RateLimitSettings {
        enabled: false,
        auth_per_sec: 0,
        auth_burst: 0,
        view_per_sec: 0,
        view_burst: 0,
        upload_per_sec: 0,
        upload_burst: 0,
        global_per_sec: 0,
        global_burst: 0,
        ..RateLimitSettings::default()
    });
    let router = Router::new().route("/", get(|| async { "ok" }));
    for app in [
        layers.layer_auth(router.clone()),
        layers.layer_view(router.clone()),
        layers.layer_upload(router.clone()),
        layers.layer_global(router),
    ] {
        for _ in 0..10 {
            let response = app
                .clone()
                .oneshot(Request::new(Body::empty()))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
    }
}

#[tokio::test]
async fn http_auth_limiter_enforces_the_burst_and_keeps_ips_independent() {
    let layers = RateLimitLayers::from_settings(RateLimitSettings {
        auth_per_sec: 1,
        auth_burst: 2,
        ..RateLimitSettings::default()
    });
    let app = layers.layer_auth(Router::new().route("/", get(|| async { "ok" })));
    for (ip, expected) in [
        ([127, 0, 0, 1], StatusCode::OK),
        ([127, 0, 0, 1], StatusCode::OK),
        ([127, 0, 0, 1], StatusCode::TOO_MANY_REQUESTS),
        ([127, 0, 0, 2], StatusCode::OK),
    ] {
        let request = Request::builder()
            .uri("/")
            .extension(ConnectInfo(SocketAddr::from((ip, 12345))))
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), expected);
    }
}
