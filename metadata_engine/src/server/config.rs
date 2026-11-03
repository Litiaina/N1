use super::error::{ApiError, ApiResult};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

/// Original INI controls are retained. The optional component section specifies
/// its database and the independent object's native endpoint and credentials.
pub struct ServerConfig {
    pub http: SocketAddr,
    pub native: SocketAddr,
    pub native_max_connections: usize,
    pub cert_path: Option<PathBuf>,
    pub key_path: Option<PathBuf>,
    pub data_dir: PathBuf,
    pub staging_dir: PathBuf,
    pub object_native_url: String,
    pub object_fragment: String,
    pub object_secret: String,
    pub object_ca: Option<PathBuf>,
    pub object_connections: usize,
    pub jwt_secret: String,
    pub master_key: String,
    pub admin_key: String,
    pub access_ttl: u64,
    pub access_max_ttl: u64,
    pub refresh_ttl: u64,
    pub metadata_workers: usize,
    pub upload_concurrency: usize,
    pub upload_memory_budget: usize,
    pub max_file_size: Option<u64>,
    pub max_part_size: u64,
    pub strict_parts: bool,
    pub trash_retention_seconds: u64,
    pub file_ttl_seconds: u64,
    pub export: cli_core::settings::Export,
}
impl ServerConfig {
    /// No configured cap still preserves LINK's source-length representation.
    pub(crate) fn check_object_size(&self, bytes: u64) -> ApiResult<()> {
        let message = if self.max_file_size.is_some_and(|limit| bytes > limit) {
            Some("object exceeds configured max_file_size")
        } else if bytes > i64::MAX as u64 {
            Some("object exceeds LINK source-length limit")
        } else {
            None
        };
        match message {
            Some(message) => Err(ApiError(
                axum::http::StatusCode::PAYLOAD_TOO_LARGE,
                message.into(),
            )),
            None => Ok(()),
        }
    }

    pub fn load(path: &Path) -> ApiResult<Self> {
        let settings = cli_core::settings::N1Settings::load_metadata(path)
            .map_err(|e| ApiError::invalid(e.to_string()))?;
        let env = settings
            .read_env()
            .map_err(|_| ApiError::invalid("invalid or incomplete service credentials"))?;
        let ini = ini::Ini::load_from_file(path)
            .map_err(|_| ApiError::invalid("cannot read metadata INI"))?;
        let section = ini.section(Some("metadata_engine"));
        let get = |name: &str| section.and_then(|s| s.get(name)).map(str::to_owned);
        let parent = settings.source.parent().unwrap_or(Path::new("."));
        let resolve = |value: String| {
            let p = PathBuf::from(value);
            if p.is_absolute() { p } else { parent.join(p) }
        };
        let integer = |name: &str, fallback: u64| -> ApiResult<u64> {
            get(name)
                .map(|v| {
                    v.parse()
                        .map_err(|_| ApiError::invalid(format!("invalid metadata {name}")))
                })
                .unwrap_or(Ok(fallback))
        };
        let ttl = |name: &str, fallback: u64| -> ApiResult<u64> {
            env.get(name)
                .map(|v| {
                    v.parse()
                        .map_err(|_| ApiError::invalid("invalid token lifetime"))
                })
                .unwrap_or(Ok(fallback))
        };
        let secret_env = get("object_secret_env").unwrap_or_else(|| "AUTH_KEY".into());
        let object_secret = env
            .get(&secret_env)
            .cloned()
            .or_else(|| std::env::var(&secret_env).ok())
            .ok_or_else(|| ApiError::invalid("object service credential is missing"))?;
        let result = Self {
            http: SocketAddr::new(settings.server.host, settings.server.https_port),
            native: SocketAddr::new(settings.server.host, settings.server.native_port),
            native_max_connections: settings.server.native_max_connections,
            cert_path: settings.server.cert_path,
            key_path: settings.server.key_path,
            data_dir: get("data_dir")
                .map(resolve)
                .unwrap_or_else(|| settings.control_dir.join("metadata")),
            staging_dir: get("staging_dir")
                .map(resolve)
                .unwrap_or_else(|| settings.control_dir.join("metadata-uploads")),
            object_native_url: get("object_native_url").ok_or_else(|| {
                ApiError::invalid("[metadata_engine] object_native_url is required")
            })?,
            object_fragment: get("object_fragment").unwrap_or_else(|| "root".into()),
            object_secret,
            object_ca: get("object_ca_path").filter(|v| !v.is_empty()).map(resolve),
            object_connections: integer(
                "object_connections",
                settings.server.native_max_connections as u64,
            )? as usize,
            jwt_secret: env["JWT_SECRET"].clone(),
            master_key: env["MASTER_KEY"].clone(),
            admin_key: env["ADMIN_KEY"].clone(),
            access_ttl: ttl(
                "ACCESS_TOKEN_DEFAULT_TTL_SECS",
                settings.login_token_expiration,
            )?,
            access_max_ttl: ttl("ACCESS_TOKEN_MAX_TTL_SECS", 3600)?,
            refresh_ttl: ttl("REFRESH_TOKEN_TTL_SECS", 604800)?,
            metadata_workers: integer("workers", 64)? as usize,
            upload_concurrency: settings.upload.max_concurrent_io,
            upload_memory_budget: settings.upload.upload_memory_budget_bytes,
            max_file_size: settings.max_file_size,
            max_part_size: settings.upload.max_part_size,
            strict_parts: settings.upload.upload_part_strict_durability_default,
            trash_retention_seconds: settings.garbage_collector.trash_retention_seconds,
            file_ttl_seconds: settings.garbage_collector.file_ttl_seconds,
            export: settings.export,
        };
        if result.access_ttl == 0
            || result.access_ttl > result.access_max_ttl
            || result.access_max_ttl > 86400
            || result.refresh_ttl == 0
            || result.object_connections == 0
            || result.object_connections > 4096
            || result.upload_concurrency == 0
            || result.upload_concurrency > 4096
            || !(1..=1024).contains(&result.metadata_workers)
        {
            return Err(ApiError::invalid(
                "invalid metadata resource or token limits",
            ));
        }
        Ok(result)
    }
}
