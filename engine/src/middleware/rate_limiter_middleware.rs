use crate::middleware::auth_middleware::Claims;
use axum::Router;
use axum::body::Body;
use axum::http::Request;
use governor::middleware::NoOpMiddleware;
use std::time::Duration;
use tower_governor::GovernorLayer;
use tower_governor::errors::GovernorError;
use tower_governor::governor::{GovernorConfig, GovernorConfigBuilder};
use tower_governor::key_extractor::KeyExtractor;
use tower_governor::key_extractor::SmartIpKeyExtractor;

#[derive(Clone)]
pub struct UserKeyExtractor;

impl KeyExtractor for UserKeyExtractor {
    type Key = String;

    fn extract<B>(&self, req: &Request<B>) -> Result<Self::Key, GovernorError> {
        if req.method() == axum::http::Method::OPTIONS {
            return Ok("global_options_allow".to_string());
        }

        req.extensions()
            .get::<Claims>()
            .map(|claims| hex::encode(claims.sub.0))
            .ok_or(GovernorError::UnableToExtractKey)
    }
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RateLimitSettings {
    pub enabled: bool,
    pub auth_per_sec: u64,
    pub auth_burst: u32,
    pub view_per_sec: u64,
    pub view_burst: u32,
    pub upload_per_sec: u64,
    pub upload_burst: u32,
    pub transcode_per_sec: u64,
    pub transcode_burst: u32,
    pub global_per_sec: u64,
    pub global_burst: u32,
}

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

impl RateLimitSettings {
    pub fn validate(&self) -> crate::core::Result<()> {
        if self.enabled
            && [
                (self.auth_per_sec, self.auth_burst),
                (self.view_per_sec, self.view_burst),
                (self.upload_per_sec, self.upload_burst),
                (self.global_per_sec, self.global_burst),
            ]
            .iter()
            .any(|(rate, burst)| !(1..=1_000_000_000).contains(rate) || *burst == 0)
        {
            return Err(crate::core::Error::Invalid(
                "invalid request rate/burst".into(),
            ));
        }
        Ok(())
    }
}

/// Native and HTTP share these quotas. Conversion uses the original tested
/// middleware builder rather than a second interpretation of rates.
pub struct RateLimits {
    enabled: bool,
    auth: GovernorConfig<SmartIpKeyExtractor, NoOpMiddleware>,
    global: GovernorConfig<SmartIpKeyExtractor, NoOpMiddleware>,
    view: GovernorConfig<UserKeyExtractor, NoOpMiddleware>,
    upload: GovernorConfig<UserKeyExtractor, NoOpMiddleware>,
    last_prune: std::sync::Mutex<std::time::Instant>,
}
impl RateLimits {
    pub fn new(settings: RateLimitSettings) -> Self {
        let enabled = settings.enabled;
        let limits = if enabled {
            settings
        } else {
            RateLimitSettings::default()
        };
        Self {
            enabled,
            auth: governor_config(limits.auth_per_sec, limits.auth_burst, SmartIpKeyExtractor),
            global: governor_config(
                limits.global_per_sec,
                limits.global_burst,
                SmartIpKeyExtractor,
            ),
            view: governor_config(limits.view_per_sec, limits.view_burst, UserKeyExtractor),
            upload: governor_config(limits.upload_per_sec, limits.upload_burst, UserKeyExtractor),
            last_prune: std::sync::Mutex::new(std::time::Instant::now()),
        }
    }
    pub fn peer(&self, peer: std::net::IpAddr, authentication: bool) -> crate::core::Result<()> {
        if !self.enabled {
            return Ok(());
        }
        if let Ok(mut last) = self.last_prune.try_lock()
            && last.elapsed() >= Duration::from_secs(60)
        {
            self.auth.limiter().retain_recent();
            self.global.limiter().retain_recent();
            self.view.limiter().retain_recent();
            self.upload.limiter().retain_recent();
            *last = std::time::Instant::now();
        }
        let limiter = if authentication {
            &self.auth
        } else {
            &self.global
        };
        limiter
            .limiter()
            .check_key(&peer)
            .map_err(|_| crate::core::Error::RateLimited)
    }
    pub fn operation(
        &self,
        claims: &Claims,
        command: &n1_link::noa::Command,
    ) -> crate::core::Result<()> {
        use n1_link::noa::Command;
        if !self.enabled {
            return Ok(());
        }
        let write = matches!(
            command,
            Command::UploadInit { .. }
                | Command::UploadPut { .. }
                | Command::UploadBatch { .. }
                | Command::UploadPart { .. }
                | Command::UploadFinalize { .. }
                | Command::UploadCancel { .. }
        );
        let limiter = if write { &self.upload } else { &self.view };
        limiter
            .limiter()
            .check_key(&hex::encode(claims.sub.0))
            .map_err(|_| crate::core::Error::RateLimited)
    }
}

impl Default for RateLimitLayers {
    fn default() -> Self {
        Self::new()
    }
}

pub struct RateLimitLayers {
    pub auth: Option<GovernorLayer<SmartIpKeyExtractor, NoOpMiddleware, Body>>,
    pub view: Option<GovernorLayer<UserKeyExtractor, NoOpMiddleware, Body>>,
    pub upload: Option<GovernorLayer<UserKeyExtractor, NoOpMiddleware, Body>>,
    pub global: Option<GovernorLayer<SmartIpKeyExtractor, NoOpMiddleware, Body>>,
}

impl RateLimitLayers {
    pub fn new() -> Self {
        Self::from_settings(RateLimitSettings::default())
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
