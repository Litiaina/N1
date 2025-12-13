use std::sync::OnceLock;
use axum::Router;
use axum::body::Body;
use axum::http::Request;
use governor::middleware::NoOpMiddleware;
use ini::Ini;
use tower_governor::key_extractor::KeyExtractor;
use tower_governor::errors::GovernorError;
use tower_governor::GovernorLayer;
use tower_governor::governor::GovernorConfigBuilder;
use tower_governor::key_extractor::SmartIpKeyExtractor;
use jsonwebtoken::{decode, Validation};
use tracing::error;
use crate::structs::auth_structs::Claims;
use crate::middleware::auth_middleware::JWT_KEYS;

#[derive(Clone)]
pub struct UserKeyExtractor;

impl KeyExtractor for UserKeyExtractor {
    type Key = String;

    fn extract<B>(&self, req: &Request<B>) -> Result<Self::Key, GovernorError> {
        if req.method() == axum::http::Method::OPTIONS {
            return Ok("global_options_allow".to_string());
        }

        let auth_header = req.headers()
            .get("authorization")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "));

        if let Some(token) = auth_header {
             let validation = Validation::default();
             
             if let Ok(token_data) = decode::<Claims>(token, &JWT_KEYS.decoding, &validation) {
                 return Ok(token_data.claims.uid.to_string());
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
    sharing_per_sec: u64,
    sharing_burst: u32,
    global_per_sec: u64,
    global_burst: u32
}

static RATE_LIMIT_CONFIG: OnceLock<RateLimitSettings> = OnceLock::new();

fn get_rate_limit_config() -> RateLimitSettings {
    *RATE_LIMIT_CONFIG.get_or_init(|| {
        let defaults = RateLimitSettings {
            enabled: true,
            auth_per_sec: 2,
            auth_burst: 5,
            view_per_sec: 30,
            view_burst: 100,
            upload_per_sec: 300,
            upload_burst: 3000,
            transcode_per_sec: 1,
            transcode_burst: 2,
            sharing_per_sec: 50,
            sharing_burst: 100,
            global_per_sec: 200,
            global_burst: 300
        };

        let config_result = Ini::load_from_file("config.ini");
        
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
                            sharing_per_sec: get_val!("sharing_limit_per_second", defaults.sharing_per_sec),
                            sharing_burst: get_val!("sharing_limit_burst_size", defaults.sharing_burst),
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

pub struct RateLimitLayers {
    pub auth: Option<GovernorLayer<SmartIpKeyExtractor, NoOpMiddleware, Body>>,
    pub view: Option<GovernorLayer<UserKeyExtractor, NoOpMiddleware, Body>>,
    pub upload: Option<GovernorLayer<UserKeyExtractor, NoOpMiddleware, Body>>,
    pub transcode: Option<GovernorLayer<UserKeyExtractor, NoOpMiddleware, Body>>,
    pub sharing: Option<GovernorLayer<SmartIpKeyExtractor, NoOpMiddleware, Body>>,
    pub global: Option<GovernorLayer<SmartIpKeyExtractor, NoOpMiddleware, Body>>
}

impl RateLimitLayers {
    pub fn new() -> Self {
        let limits = get_rate_limit_config();

        if !limits.enabled {
            return Self {
                auth: None, view: None, upload: None, transcode: None, sharing: None, global: None
            };
        }

        let build_ip_conf = |per_sec, burst| {
             GovernorConfigBuilder::default()
                .per_second(per_sec)
                .burst_size(burst)
                .key_extractor(SmartIpKeyExtractor)
                .finish()
                .unwrap()
        };

        let build_user_conf = |per_sec, burst| {
             GovernorConfigBuilder::default()
                .per_second(per_sec)
                .burst_size(burst)
                .key_extractor(UserKeyExtractor)
                .finish()
                .unwrap()
        };

        Self {
            auth: Some(GovernorLayer::new(build_ip_conf(limits.auth_per_sec, limits.auth_burst))),
            view: Some(GovernorLayer::new(build_user_conf(limits.view_per_sec, limits.view_burst))),
            upload: Some(GovernorLayer::new(build_user_conf(limits.upload_per_sec, limits.upload_burst))),
            transcode: Some(GovernorLayer::new(build_user_conf(limits.transcode_per_sec, limits.transcode_burst))),
            sharing: Some(GovernorLayer::new(build_ip_conf(limits.sharing_per_sec, limits.sharing_burst))),
            global: Some(GovernorLayer::new(build_ip_conf(limits.global_per_sec, limits.global_burst))),
        }
    }

    pub fn layer_auth<S>(&self, router: Router<S>) -> Router<S> 
    where S: Clone + Send + Sync + 'static 
    {
        if let Some(layer) = &self.auth { router.layer(layer.clone()) } else { router }
    }

    pub fn layer_view<S>(&self, router: Router<S>) -> Router<S> 
    where S: Clone + Send + Sync + 'static 
    {
        if let Some(layer) = &self.view { router.layer(layer.clone()) } else { router }
    }

    pub fn layer_upload<S>(&self, router: Router<S>) -> Router<S> 
    where S: Clone + Send + Sync + 'static 
    {
        if let Some(layer) = &self.upload { router.layer(layer.clone()) } else { router }
    }

    pub fn layer_transcode<S>(&self, router: Router<S>) -> Router<S> 
    where S: Clone + Send + Sync + 'static 
    {
        if let Some(layer) = &self.transcode { router.layer(layer.clone()) } else { router }
    }

    pub fn layer_sharing<S>(&self, router: Router<S>) -> Router<S> 
    where S: Clone + Send + Sync + 'static 
    {
        if let Some(layer) = &self.sharing { router.layer(layer.clone()) } else { router }
    }

    pub fn layer_global<S>(&self, router: Router<S>) -> Router<S> 
    where S: Clone + Send + Sync + 'static 
    {
        if let Some(layer) = &self.global { router.layer(layer.clone()) } else { router }
    }
}
