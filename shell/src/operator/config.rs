//! Operator configuration and authentication retain the original N1 host
//! contract (061a40a, core/backup/network.rs). Storage work stays in the engine.
use cli_core::Result;
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Store {
    pub path: PathBuf,
    pub key_file: PathBuf,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Destination {
    /// Current native NOA credentials, never a browser-supplied token or path.
    pub credentials_file: PathBuf,
    pub repository: String,
    #[serde(default)]
    pub ca_file: Option<PathBuf>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    pub bind: std::net::SocketAddr,
    pub token_file: PathBuf,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    #[serde(default)]
    pub allow_plaintext: bool,
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    pub repositories: BTreeMap<String, Store>,
    pub engine_config: Option<PathBuf>,
    pub engine_binary: Option<PathBuf>,
    #[serde(default)]
    pub restore_targets: BTreeMap<String, PathBuf>,
    #[serde(default)]
    pub replicas: BTreeMap<String, Destination>,
    #[serde(default)]
    pub jobs: BTreeMap<String, PathBuf>,
    pub online_source: Option<Destination>,
    #[serde(default = "boundary")]
    pub capture_boundary_seconds: u64,
}
fn boundary() -> u64 {
    30
}
pub(super) fn private_file(path: &Path, limit: u64) -> Result<Vec<u8>> {
    use std::{
        io::Read,
        os::unix::fs::{OpenOptionsExt, PermissionsExt},
    };
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file() || meta.permissions().mode() & 0o077 != 0 || meta.len() > limit {
        return Err("operator config/token must be an owner-only bounded regular file".into());
    }
    let mut bytes = Vec::new();
    file.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err("operator file grew beyond its limit".into());
    }
    Ok(bytes)
}
pub(super) fn token(path: &Path) -> Result<String> {
    let token = String::from_utf8(private_file(path, 1024)?)?
        .trim()
        .to_owned();
    if token.len() < 32 || !token.bytes().all(|b| b.is_ascii_graphic()) {
        return Err("operator token needs at least 32 printable non-space characters".into());
    }
    Ok(token)
}
pub(crate) fn generate_token(path: &Path) -> Result<()> {
    use rand::RngCore;
    use std::{io::Write, os::unix::fs::OpenOptionsExt};
    let mut bytes = [0; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    file.write_all(hex::encode(bytes).as_bytes())?;
    file.sync_all()?;
    std::fs::File::open(
        path.parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?
    .sync_all()?;
    Ok(())
}
pub(super) fn alias(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
}
pub(super) fn identity(value: &str) -> Result<()> {
    if ulid::Ulid::from_string(value).is_ok_and(|id| id.to_string() == value) {
        Ok(())
    } else {
        Err("invalid recovery-point identity".into())
    }
}
impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let value: Self = serde_json::from_slice(&private_file(path, 65536)?)?;
        value.validate()?;
        Ok(value)
    }
    fn validate(&self) -> Result<()> {
        if self.repositories.is_empty()
            || self.repositories.len() > 32
            || self.replicas.len() > 32
            || self.restore_targets.len() > 32
            || self.jobs.len() > 32
            || self.allowed_origins.len() > 32
            || !(1..=300).contains(&self.capture_boundary_seconds)
            || self
                .repositories
                .keys()
                .chain(self.replicas.keys())
                .chain(self.restore_targets.keys())
                .chain(self.jobs.keys())
                .any(|name| !alias(name))
        {
            return Err("invalid operator aliases or bounds".into());
        }
        if self.tls_cert.is_some() != self.tls_key.is_some()
            || self.tls_cert.is_none() && !self.allow_plaintext
        {
            return Err("operator listener requires TLS or explicit allow_plaintext".into());
        }
        for origin in &self.allowed_origins {
            let uri: axum::http::Uri = origin.parse()?;
            let scheme = uri.scheme_str().ok_or("operator origin needs a scheme")?;
            let authority = uri.authority().ok_or("operator origin needs a host")?;
            if !matches!(scheme, "http" | "https")
                || authority.as_str().contains('@')
                || *origin != format!("{scheme}://{authority}")
            {
                return Err("operator origins must be exact HTTP(S) origins".into());
            }
        }
        let mut paths = Vec::new();
        for repo in self.repositories.values() {
            paths.extend([&repo.path, &repo.key_file]);
        }
        paths.extend(self.restore_targets.values());
        paths.extend(self.jobs.values());
        paths.extend(self.engine_config.iter());
        paths.extend(self.engine_binary.iter());
        paths.extend(self.tls_cert.iter());
        paths.extend(self.tls_key.iter());
        paths.push(&self.token_file);
        for destination in self.replicas.values().chain(self.online_source.iter()) {
            if !alias(&destination.repository) {
                return Err("invalid native repository alias".into());
            }
            paths.push(&destination.credentials_file);
            paths.extend(destination.ca_file.iter());
            let credentials = cli_core::Credentials::load(&destination.credentials_file)?;
            n1_native_client::parse_native_address(
                credentials
                    .native_url
                    .as_deref()
                    .ok_or("native destination URL is required")?,
            )?;
        }
        if paths.into_iter().any(|p| !p.is_absolute()) {
            return Err("operator paths must be absolute".into());
        }
        Ok(())
    }
    pub fn engine(&self) -> Result<PathBuf> {
        Ok(match &self.engine_binary {
            Some(path) => path.clone(),
            None => std::env::current_exe()?
                .parent()
                .ok_or("shell has no parent directory")?
                .join("litiaina-n1"),
        })
    }
}
