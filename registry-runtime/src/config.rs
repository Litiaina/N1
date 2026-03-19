use std::fs;
use std::env;
use std::path::PathBuf;

use crate::create_env::create_registry_env_file;

#[derive(Clone, Debug)]
pub struct RegistryConfig {
    pub host: String,
    pub https_port: u16,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub sqlite_path: PathBuf,
    pub jwt_secret: String,
    pub master_key: String,
    pub access_token_ttl_secs: u64,
    pub default_quota_bytes: i64,
    pub storage_session_ttl_secs: u64,
    pub single_node_url: Option<String>,
    pub single_node_master_key: Option<String>,
    pub single_node_insecure_tls: bool,
    pub multi_node_url: Option<String>,
    pub multi_node_master_key: Option<String>,
    pub multi_node_insecure_tls: bool,
}

impl RegistryConfig {
    pub fn load() -> Result<Self, String> {
        ensure_registry_env_file().map_err(|err| format!("failed to ensure registry.env: {}", err))?;
        let _ = dotenv::from_filename("registry.env");
        let _ = dotenv::dotenv();

        let host = env_or("REGISTRY_HOST", "0.0.0.0");
        let https_port = parse_or("REGISTRY_HTTPS_PORT", 51001u16)?;
        let cert_path = PathBuf::from(env_or("REGISTRY_CERT_PATH", "certificates/cert.pem"));
        let key_path = PathBuf::from(env_or("REGISTRY_KEY_PATH", "certificates/key.pem"));
        let sqlite_path = PathBuf::from(env_or("REGISTRY_SQLITE_PATH", "registry.db"));
        let jwt_secret = required("REGISTRY_JWT_SECRET")?;
        let master_key = required("REGISTRY_MASTER_KEY")?;
        let access_token_ttl_secs = parse_or("REGISTRY_ACCESS_TOKEN_TTL_SECS", 900u64)?;
        let default_quota_bytes = parse_or(
            "REGISTRY_DEFAULT_QUOTA_BYTES",
            25_i64 * 1024 * 1024 * 1024,
        )?;
        let storage_session_ttl_secs =
            parse_or("REGISTRY_STORAGE_SESSION_TTL_SECS", 15 * 60u64)?;
        let single_node_url = optional("REGISTRY_SINGLE_NODE_URL")
            .map(|value| {
                normalize_backend_url(&value)
                    .map_err(|err| format!("invalid REGISTRY_SINGLE_NODE_URL: {}", err))
            })
            .transpose()?;
        let single_node_master_key = optional("REGISTRY_SINGLE_NODE_MASTER_KEY");
        let single_node_insecure_tls = parse_or("REGISTRY_SINGLE_NODE_INSECURE_TLS", false)?;
        let multi_node_url = optional("REGISTRY_MULTI_NODE_URL")
            .map(|value| {
                normalize_backend_url(&value)
                    .map_err(|err| format!("invalid REGISTRY_MULTI_NODE_URL: {}", err))
            })
            .transpose()?;
        let multi_node_master_key = optional("REGISTRY_MULTI_NODE_MASTER_KEY");
        let multi_node_insecure_tls = parse_or("REGISTRY_MULTI_NODE_INSECURE_TLS", false)?;

        Ok(Self {
            host,
            https_port,
            cert_path,
            key_path,
            sqlite_path,
            jwt_secret,
            master_key,
            access_token_ttl_secs,
            default_quota_bytes,
            storage_session_ttl_secs,
            single_node_url,
            single_node_master_key,
            single_node_insecure_tls,
            multi_node_url,
            multi_node_master_key,
            multi_node_insecure_tls,
        })
    }
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn required(key: &str) -> Result<String, String> {
    match env::var(key) {
        Ok(value) if !value.trim().is_empty() => Ok(value),
        _ => Err(format!(
            "missing required env var {} (set it in registry.env or export it in the environment)",
            key
        )),
    }
}

fn optional(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn parse_or<T>(key: &str, default: T) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match env::var(key) {
        Ok(value) => value
            .parse::<T>()
            .map_err(|err| format!("invalid {}: {}", key, err)),
        Err(_) => Ok(default),
    }
}

fn ensure_registry_env_file() -> std::io::Result<()> {
    let env_path = PathBuf::from("registry.env");
    if env_path.exists() {
        return Ok(());
    }

    if let Some(parent) = env_path.parent() {
        fs::create_dir_all(parent)?;
    }

    create_registry_env_file(&env_path)
}

fn repair_common_backend_url_typos(raw: &str) -> String {
    let trimmed = raw.trim();
    let Some((scheme, remainder)) = trimmed.split_once("://") else {
        return trimmed.to_string();
    };
    let authority_end = remainder
        .find(['/', '?', '#'])
        .unwrap_or(remainder.len());
    let authority = &remainder[..authority_end];
    let suffix = &remainder[authority_end..];

    if authority.contains(':') || !authority.starts_with("localhost") {
        return trimmed.to_string();
    }

    let port_suffix = &authority["localhost".len()..];
    if port_suffix.is_empty() || !port_suffix.chars().all(|ch| ch.is_ascii_digit()) {
        return trimmed.to_string();
    }

    format!("{}://localhost:{}{}", scheme, port_suffix, suffix)
}

pub fn normalize_backend_url(raw: &str) -> Result<String, String> {
    let candidate = repair_common_backend_url_typos(raw);
    let parsed = reqwest::Url::parse(candidate.trim()).map_err(|err| {
        format!(
            "expected a valid backend URL like https://localhost:50001 (got '{}'): {}",
            raw.trim(),
            err
        )
    })?;

    match parsed.scheme() {
        "http" | "https" => {}
        other => {
            return Err(format!(
                "unsupported backend URL scheme '{}' (expected http or https)",
                other
            ));
        }
    }

    let host = parsed
        .host_str()
        .ok_or_else(|| "backend URL is missing a host".to_string())?;
    if host.starts_with("localhost")
        && host.len() > "localhost".len()
        && host["localhost".len()..]
            .chars()
            .all(|ch| ch.is_ascii_digit())
        && parsed.port().is_none()
    {
        return Err(format!(
            "backend URL '{}' looks like a missing-colon typo; use https://localhost:{}",
            raw.trim(),
            &host["localhost".len()..]
        ));
    }

    Ok(parsed.as_str().trim_end_matches('/').to_string())
}

#[cfg(test)]
mod tests {
    use super::normalize_backend_url;

    #[test]
    fn normalizes_backend_url_and_trims_trailing_slash() {
        let normalized =
            normalize_backend_url("https://localhost:50001/").expect("normalize backend url");
        assert_eq!(normalized, "https://localhost:50001");
    }

    #[test]
    fn repairs_missing_colon_in_localhost_backend_url() {
        let normalized =
            normalize_backend_url("https://localhost50001").expect("repair localhost typo");
        assert_eq!(normalized, "https://localhost:50001");
    }

    #[test]
    fn rejects_non_http_backend_url_scheme() {
        let err = normalize_backend_url("ftp://localhost:50001").expect_err("reject ftp");
        assert!(err.contains("unsupported backend URL scheme"));
    }
}
