use crate::middleware::auth_middleware::JWT_KEYS;
use crate::settings::constants::CONFIG_FILE;
use crate::structs::auth_structs::Claims;
use axum::Router;
use axum::body::Body;
use axum::http::Request;
use governor::middleware::NoOpMiddleware;
use ini::Ini;
use jsonwebtoken::{Validation, decode};
use std::sync::OnceLock;
use std::time::Duration;
use tower_governor::GovernorLayer;
use tower_governor::errors::GovernorError;
use tower_governor::governor::{GovernorConfig, GovernorConfigBuilder};
use tower_governor::key_extractor::KeyExtractor;
use tower_governor::key_extractor::SmartIpKeyExtractor;
use tracing::error;

#[derive(Clone)]
pub struct UserKeyExtractor;

impl KeyExtractor for UserKeyExtractor {
    type Key = String;

    fn extract<B>(&self, req: &Request<B>) -> Result<Self::Key, GovernorError> {
        if req.method() == axum::http::Method::OPTIONS {
            return Ok("global_options_allow".to_string());
        }

        let auth_header = req
            .headers()
            .get("authorization")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "));

        if let Some(token) = auth_header {
            let validation = Validation::default();

            if let Ok(token_data) = decode::<Claims>(token, &JWT_KEYS.decoding, &validation) {
                return Ok(token_data.claims.fragment.to_string());
            }
        }

        Err(GovernorError::UnableToExtractKey)
    }
}

#[derive(Clone, Copy, Debug)]
struct RateLimitSettings {
    enabled: bool,
    auth_per_sec: u64,
    auth_burst: u32,
    view_per_sec: u64,
    view_burst: u32,
    upload_per_sec: u64,
    upload_burst: u32,
    transcode_per_sec: u64,
    transcode_burst: u32,
    global_per_sec: u64,
    global_burst: u32,
}

static RATE_LIMIT_CONFIG: OnceLock<RateLimitSettings> = OnceLock::new();

impl Default for RateLimitSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            auth_per_sec: 2,
            auth_burst: 5,
            view_per_sec: 30,
            view_burst: 100,
            upload_per_sec: 300,
            upload_burst: 3000,
            transcode_per_sec: 1,
            transcode_burst: 2,
            global_per_sec: 200,
            global_burst: 300,
        }
    }
}

fn get_rate_limit_config() -> RateLimitSettings {
    *RATE_LIMIT_CONFIG.get_or_init(|| {
        let defaults = RateLimitSettings::default();

        let config_result = Ini::load_from_file(CONFIG_FILE);

        match config_result {
            Ok(config) => {
                let section = config.section(Some("rate_limiter_config"));
                match section {
                    Some(props) => {
                         macro_rules! get_val {
                            ($key:expr, $default:expr) => {
                                props.get($key)
                                    .and_then(|v| v.parse().ok())
                                    .unwrap_or($default)
                            };
                        }

                        let enabled = props.get("enable_rate_limiting")
                            .map(|v| v.to_lowercase())
                            .map(|v| match v.as_str() {
                                "true" | "1" | "on" => true,
                                "false" | "0" | "off" => false,
                                _ => defaults.enabled,
                            })
                            .unwrap_or(defaults.enabled);

                        RateLimitSettings {
                            enabled,
                            auth_per_sec: get_val!("auth_limit_per_second", defaults.auth_per_sec),
                            auth_burst: get_val!("auth_limit_burst_size", defaults.auth_burst),
                            view_per_sec: get_val!("view_limit_per_second", defaults.view_per_sec),
                            view_burst: get_val!("view_limit_burst_size", defaults.view_burst),
                            upload_per_sec: get_val!("upload_limit_per_second", defaults.upload_per_sec),
                            upload_burst: get_val!("upload_limit_burst_size", defaults.upload_burst),
                            transcode_per_sec: get_val!("transcode_limit_per_second", defaults.transcode_per_sec),
                            transcode_burst: get_val!("transcode_limit_burst_size", defaults.transcode_burst),
                            global_per_sec: get_val!("global_limit_per_second", defaults.global_per_sec),
                            global_burst: get_val!("global_limit_burst_size", defaults.global_burst),
                        }
                    },
                    None => {
                        error!("Warning: [rate_limiter_config] section missing in config.ini. Using defaults.");
                        defaults
                    }
                }
            },
            Err(_) => {
                error!("Warning: config.ini not found. Using default rate limits.");
                defaults
            }
        }
    })
}

fn governor_config<K: KeyExtractor>(
    requests_per_second: u64,
    burst: u32,
    key_extractor: K,
) -> GovernorConfig<K, NoOpMiddleware> {
    assert!(
        (1..=1_000_000_000).contains(&requests_per_second),
        "rate_limiter_config: requests per second must be between 1 and 1000000000"
    );
    assert!(burst > 0, "rate_limiter_config: burst size must be nonzero");
    // tower_governor's per_second(n) means one token every n seconds, not n RPS.
    // Round the reciprocal up to a nanosecond, never exceeding the requested rate.
    let period = Duration::from_nanos(1_000_000_000_u64.div_ceil(requests_per_second));
    GovernorConfigBuilder::default()
        .period(period)
        .burst_size(burst)
        .key_extractor(key_extractor)
        .finish()
        .expect("validated rate and burst must produce a nonzero quota")
}

pub struct RateLimitLayers {
    pub auth: Option<GovernorLayer<SmartIpKeyExtractor, NoOpMiddleware, Body>>,
    pub view: Option<GovernorLayer<UserKeyExtractor, NoOpMiddleware, Body>>,
    pub upload: Option<GovernorLayer<UserKeyExtractor, NoOpMiddleware, Body>>,
    pub global: Option<GovernorLayer<SmartIpKeyExtractor, NoOpMiddleware, Body>>,
}

impl RateLimitLayers {
    pub fn new() -> Self {
        Self::from_settings(get_rate_limit_config())
    }

    fn from_settings(limits: RateLimitSettings) -> Self {
        if !limits.enabled {
            return Self {
                auth: None,
                view: None,
                upload: None,
                global: None,
            };
        }

        Self {
            auth: Some(GovernorLayer::new(governor_config(
                limits.auth_per_sec,
                limits.auth_burst,
                SmartIpKeyExtractor,
            ))),
            view: Some(GovernorLayer::new(governor_config(
                limits.view_per_sec,
                limits.view_burst,
                UserKeyExtractor,
            ))),
            upload: Some(GovernorLayer::new(governor_config(
                limits.upload_per_sec,
                limits.upload_burst,
                UserKeyExtractor,
            ))),
            global: Some(GovernorLayer::new(governor_config(
                limits.global_per_sec,
                limits.global_burst,
                SmartIpKeyExtractor,
            ))),
        }
    }

    pub fn layer_auth<S>(&self, router: Router<S>) -> Router<S>
    where
        S: Clone + Send + Sync + 'static,
    {
        if let Some(layer) = &self.auth {
            router.layer(layer.clone())
        } else {
            router
        }
    }

    pub fn layer_view<S>(&self, router: Router<S>) -> Router<S>
    where
        S: Clone + Send + Sync + 'static,
    {
        if let Some(layer) = &self.view {
            router.layer(layer.clone())
        } else {
            router
        }
    }

    pub fn layer_upload<S>(&self, router: Router<S>) -> Router<S>
    where
        S: Clone + Send + Sync + 'static,
    {
        if let Some(layer) = &self.upload {
            router.layer(layer.clone())
        } else {
            router
        }
    }

    pub fn layer_global<S>(&self, router: Router<S>) -> Router<S>
    where
        S: Clone + Send + Sync + 'static,
    {
        if let Some(layer) = &self.global {
            router.layer(layer.clone())
        } else {
            router
        }
    }
}

#[cfg(test)]
mod tests;
