//! Effective object service configuration. Operator INI and recovery JSON share
//! these settings without changing LINK's object identity or storage format.
use crate::core::{Error, Result, storage_pool::Pool};
use n1_link::{PoolId, PoolLayout, noa::Permissions};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::Read,
    net::SocketAddr,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

/// Identifies this configuration schema, independently of the binary release.
pub const CONFIG_FORMAT: &str = "N1-LINK";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub format: String,
    pub native_address: SocketAddr,
    pub http_address: Option<SocketAddr>,
    pub signing_key_file: PathBuf,
    #[serde(default)]
    pub state_dir: Option<PathBuf>,
    pub max_connections: usize,
    pub max_operations: usize,
    pub operation_memory_bytes: usize,
    pub read_repair: bool,
    #[serde(default)]
    pub concurrency: ConcurrencyConfig,
    #[serde(default)]
    pub rate_limits: crate::middleware::rate_limiter_middleware::RateLimitSettings,
    #[serde(default)]
    pub repositories:
        std::collections::BTreeMap<String, crate::core::backup::network::ConfiguredRepository>,
    #[serde(default)]
    pub maintenance: crate::core::maintenance::jobs::MaintenanceConfig,
    #[serde(default)]
    pub runtime: super::runtime::RuntimeConfig,
    pub tls: Option<Tls>,
    pub pools: Vec<ConfiguredPool>,
    pub fragments: Vec<Fragment>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConcurrencyConfig {
    pub adaptive_enabled: bool,
    pub interval_ms: u64,
    pub max_streams: usize,
    pub stream_chunk_bytes: usize,
    pub stream_readahead_bytes: usize,
}
impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self {
            adaptive_enabled: true,
            interval_ms: 1000,
            max_streams: 128,
            stream_chunk_bytes: 256 * 1024,
            stream_readahead_bytes: 512 * 1024,
        }
    }
}
impl ConcurrencyConfig {
    pub fn configure(&self, max_io: usize, members: usize) -> Result<()> {
        use crate::core::{
            adaptive_concurrency as adaptive, io_scheduler, object_parser as stream, upload,
        };
        use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
        let configured = |name: &str| Error::Conflict(format!("runtime {name} already configured"));
        io_scheduler::configure_disk_io_scheduler(
            io_scheduler::DiskIoSchedulerConfig::from_runtime(max_io, members),
        )
        .map_err(|e| Error::Conflict(e.into()))?;
        upload::MAX_IO_SEMAPHORE
            .set(max_io)
            .map_err(|_| configured("I/O limit"))?;
        upload::ADAPTIVE_IO_TARGET_ATOMIC
            .set(AtomicUsize::new(max_io))
            .map_err(|_| configured("I/O target"))?;
        adaptive::ADAPTIVE_CONCURRENCY_ENABLED_ATOMIC
            .set(AtomicBool::new(self.adaptive_enabled))
            .map_err(|_| configured("adaptive enable"))?;
        adaptive::ADAPTIVE_CONCURRENCY_INTERVAL_MS_ATOMIC
            .set(AtomicU64::new(self.interval_ms))
            .map_err(|_| configured("adaptive interval"))?;
        stream::STREAM_MAX_CONCURRENCY
            .set(self.max_streams)
            .map_err(|_| configured("stream limit"))?;
        stream::ADAPTIVE_STREAM_TARGET_ATOMIC
            .set(AtomicUsize::new(self.max_streams))
            .map_err(|_| configured("stream target"))?;
        stream::STREAM_CHUNK_SIZE_ATOMIC
            .set(AtomicUsize::new(self.stream_chunk_bytes))
            .map_err(|_| configured("stream chunk"))?;
        stream::STREAM_READAHEAD_BYTES_ATOMIC
            .set(AtomicUsize::new(self.stream_readahead_bytes))
            .map_err(|_| configured("stream read ahead"))?;
        Ok(())
    }
    fn validate(&self) -> Result<()> {
        if !(1000..=300000).contains(&self.interval_ms)
            || !(1..=4096).contains(&self.max_streams)
            || !(4096..=4 * 1024 * 1024).contains(&self.stream_chunk_bytes)
            || !(64 * 1024..=8 * 1024 * 1024).contains(&self.stream_readahead_bytes)
            || self.stream_readahead_bytes < self.stream_chunk_bytes
        {
            return Err(Error::Invalid(
                "invalid adaptive concurrency/stream bounds".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    pub certificate: PathBuf,
    pub private_key: PathBuf,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredPool {
    pub layout: PoolLayout,
    pub members: Vec<PoolId>,
    pub roots: Vec<PathBuf>,
    pub sealed: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fragment {
    pub id: PoolId,
    pub alias: String,
    pub secret_hash: String,
    pub permissions: Permissions,
    pub operator: bool,
}

impl Config {
    pub fn state_root(&self) -> PathBuf {
        self.state_dir
            .clone()
            .unwrap_or_else(|| self.signing_key_file.with_extension("state"))
    }
    pub fn ensure_state_root(&self) -> Result<PathBuf> {
        use std::os::unix::fs::DirBuilderExt;
        let path = self.state_root();
        let new = match std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&path)
        {
            Ok(()) => true,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => false,
            Err(e) => return Err(e.into()),
        };
        let meta = path.symlink_metadata()?;
        if !meta.is_dir() || meta.permissions().mode() & 0o077 != 0 {
            return Err(Error::Invalid(
                "engine state must be an owner-only directory".into(),
            ));
        }
        if new {
            File::open(
                path.parent()
                    .ok_or_else(|| Error::Invalid("state directory has no parent".into()))?,
            )?
            .sync_all()?;
        }
        Ok(path)
    }
    pub fn load(path: &Path) -> Result<Self> {
        if super::ini::is_ini(path) {
            return super::ini::load(path);
        }
        let file = open_private(path)?;
        if file.metadata()?.len() > 4 * 1024 * 1024 {
            return Err(Error::Invalid("configuration file exceeds 4 MiB".into()));
        }
        let cfg: Self = serde_json::from_reader(file)
            .map_err(|e| Error::Invalid(format!("invalid N1 configuration: {e}")))?;
        cfg.validate()?;
        Ok(cfg)
    }
    pub fn validate(&self) -> Result<()> {
        if !self.signing_key_file.is_absolute() || !self.state_root().is_absolute() {
            return Err(Error::Invalid(
                "signing key/state paths must be absolute".into(),
            ));
        }
        if self.format != CONFIG_FORMAT {
            return Err(Error::Invalid("unsupported N1 configuration format".into()));
        }
        if self.pools.is_empty() || self.fragments.is_empty() {
            return Err(Error::Invalid(
                "configuration requires at least one storage pool and fragment".into(),
            ));
        }
        if !(1..=4096).contains(&self.max_connections) || !(1..=4096).contains(&self.max_operations)
        {
            return Err(Error::Invalid("invalid connection/operation bound".into()));
        }
        self.maintenance.validate()?;
        self.runtime.validate()?;
        self.concurrency.validate()?;
        self.rate_limits.validate()?;
        if self.repositories.len() > 32 {
            return Err(Error::Invalid("too many recovery repositories".into()));
        }
        for (alias, repo) in &self.repositories {
            if !crate::core::backup::network::valid_alias(alias)
                || !repo.path.is_absolute()
                || !repo.key_file.is_absolute()
            {
                return Err(Error::Invalid(
                    "invalid recovery repository configuration".into(),
                ));
            }
        }
        let mut pools = std::collections::HashSet::new();
        let mut roots = std::collections::HashSet::new();
        for pool in &self.pools {
            pool.layout.validate()?;
            if pool.members.len() != pool.layout.width()
                || pool
                    .members
                    .iter()
                    .collect::<std::collections::HashSet<_>>()
                    .len()
                    != pool.layout.width()
            {
                return Err(Error::Invalid(
                    "configured pool must bind every immutable member identity".into(),
                ));
            }
            let download_working_set = 16 * pool.layout.width() * pool.layout.block_bytes as usize
                + 4 * 1024 * 1024
                + self.concurrency.stream_readahead_bytes;
            if download_working_set.div_ceil(65536) > self.operation_memory_bytes / 65536 {
                return Err(Error::Invalid(
                    "pool download working set exceeds operation memory budget".into(),
                ));
            }
            if !pools.insert((pool.layout.id, pool.layout.generation))
                || pool.roots.len() != pool.layout.width()
            {
                return Err(Error::Invalid(
                    "duplicate or incomplete pool configuration".into(),
                ));
            }
            for root in &pool.roots {
                if self.state_root().starts_with(root) {
                    return Err(Error::Invalid(
                        "engine control state must be outside object member roots".into(),
                    ));
                }
                if !root.is_absolute() || !roots.insert(root) {
                    return Err(Error::Invalid(
                        "member roots must be absolute and distinct".into(),
                    ));
                }
            }
        }
        let mut ids = std::collections::HashSet::new();
        let mut aliases = std::collections::HashSet::new();
        for fragment in &self.fragments {
            if fragment.alias.is_empty()
                || !ids.insert(fragment.id)
                || !aliases.insert(&fragment.alias)
                || argon2::PasswordHash::new(&fragment.secret_hash).is_err()
            {
                return Err(Error::Invalid(
                    "invalid or duplicate fragment configuration".into(),
                ));
            }
        }
        Ok(())
    }
    pub fn signing_secret(&self) -> Result<[u8; 32]> {
        let mut file = open_private(&self.signing_key_file)?;
        if file.metadata()?.len() != 32 {
            return Err(Error::Invalid(
                "signing key must contain exactly 32 bytes".into(),
            ));
        }
        let mut secret = [0; 32];
        file.read_exact(&mut secret)?;
        Ok(secret)
    }
    pub fn open_pools(&self) -> Result<Vec<Pool>> {
        self.pools
            .iter()
            .map(|p| {
                let pool = Pool::open(p.layout.clone(), &p.roots)?;
                crate::core::root_identity::verify_pool(&pool, &p.members)?;
                Ok(pool)
            })
            .collect()
    }
}

pub(super) fn open_private(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file() || meta.permissions().mode() & 0o077 != 0 {
        return Err(Error::Invalid(
            "configuration and signing keys must be regular owner-only files".into(),
        ));
    }
    Ok(file)
}
