//! Operator configuration shared by N1, M1 and administration tools.
//! INI controls are operator input; immutable enrollment identities are private
//! persisted state. Editing addresses or limits must never regenerate identities.
mod autotune;
pub mod defaults;
mod parse;
#[cfg(test)]
mod tests;

pub use defaults::create_default_config_ini;
use serde::Serialize;
use std::{
    collections::BTreeMap,
    net::IpAddr,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, Serialize)]
pub struct OperatorSettings {
    pub source: PathBuf,
    pub server: Server,
    pub control_dir: PathBuf,
    pub pools: Vec<StoragePool>,
    pub repositories: Vec<BackupRepository>,
    pub upload: Upload,
    pub io_control: IoControl,
    pub garbage_collector: GarbageCollector,
    pub auto_healing: AutoHealing,
    pub rate_limits: RateLimits,
    pub export: Export,
    /// Optional operator cap on an object's total bytes, independent of part size.
    pub max_file_size: Option<u64>,
    pub login_token_expiration: u64,
    pub index_partition_count: u16,
}
#[derive(Clone, Debug, Serialize)]
pub struct BackupRepository {
    pub alias: String,
    pub path: PathBuf,
    pub key_file: PathBuf,
}
#[derive(Clone, Debug, Serialize)]
pub struct Server {
    pub host: IpAddr,
    pub https_port: u16,
    pub native_port: u16,
    pub native_max_connections: usize,
    pub cert_path: Option<PathBuf>,
    pub key_path: Option<PathBuf>,
    pub env_file: Option<PathBuf>,
}
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct StoragePool {
    pub name: String,
    pub generation: u64,
    pub roots: Vec<PathBuf>,
    pub data_shards: u16,
    pub parity_shards: u16,
    pub sealed: bool,
}
#[derive(Clone, Debug, Serialize)]
pub struct Upload {
    pub max_concurrent_io: usize,
    pub adaptive_concurrency_enabled: bool,
    pub adaptive_concurrency_interval_ms: u64,
    pub writer_buffer_size: usize,
    pub upload_memory_budget_bytes: usize,
    pub upload_part_strict_durability_default: bool,
    pub upload_io_gate_enabled: bool,
    pub stream_max_concurrency: usize,
    pub stream_chunk_size: usize,
    pub stream_readahead_bytes: usize,
    pub stream_locality_bias_enabled: bool,
    pub monitor_interval_seconds: u64,
    pub monitor_deep_scan_interval_seconds: u64,
    pub max_part_size: u64,
    pub stripe_block_size: u32,
}
#[derive(Clone, Debug, Serialize, serde::Deserialize)]
pub struct IoControl {
    pub foreground_wait_timeout_ms: u64,
    pub stall_threshold_ms: u64,
    pub slow_io_threshold_ms: u64,
    pub latency_samples_per_class: usize,
}
#[derive(Clone, Debug, Serialize)]
pub struct GarbageCollector {
    pub cleanup_interval_seconds: u64,
    pub file_ttl_seconds: u64,
    pub trash_retention_seconds: u64,
}
#[derive(Clone, Debug, Serialize)]
pub struct AutoHealing {
    pub enabled: bool,
    pub scrub_schedule_utc: String,
    pub scheduled_scrub_pause_millis: u64,
    pub topology_check_interval_seconds: u64,
    pub topology_recovery_stability_seconds: u64,
    pub initial_scan_delay_seconds: u64,
    pub availability_scan_interval_seconds: u64,
    pub checksum_scrub_interval_seconds: u64,
    pub object_scan_concurrency: usize,
    pub shard_repair_concurrency: usize,
    pub metadata_batch_size: usize,
}
#[derive(Clone, Debug, Serialize)]
pub struct RateLimits {
    pub enable_rate_limiting: bool,
    pub auth_limit_per_second: u64,
    pub auth_limit_burst_size: u32,
    pub view_limit_per_second: u64,
    pub view_limit_burst_size: u32,
    pub upload_limit_per_second: u64,
    pub upload_limit_burst_size: u32,
    pub global_limit_per_second: u64,
    pub global_limit_burst_size: u32,
}
#[derive(Clone, Debug, Serialize)]
pub struct Export {
    pub retention_seconds: u64,
    pub cleanup_interval_seconds: u64,
    pub max_concurrent_jobs: usize,
    pub max_queued_jobs: usize,
    pub manifest_validation_concurrency: usize,
    pub object_read_concurrency: usize,
    pub artifact_chunk_size_bytes: usize,
    pub artifact_write_concurrency: usize,
    pub max_buffered_bytes_per_job: usize,
    pub max_buffered_bytes_global: usize,
    pub max_concurrent_downloads: usize,
    pub max_retained_bytes: u64,
}
impl OperatorSettings {
    pub fn load(path: &Path) -> crate::Result<Self> {
        parse::load(path)
    }
    /// Metadata retains the common operator controls without opening or
    /// requiring physical member roots. Pool profiles come from native NOA.
    pub fn load_metadata(path: &Path) -> crate::Result<Self> {
        parse::load_metadata(path)
    }
    /// Read this service's selected secrets file, or its named adjacent default.
    /// N1 uses `n1.env`; M1 uses `m1.env`. Service environment values take priority.
    pub fn read_env(&self, default_file: &str) -> crate::Result<BTreeMap<String, String>> {
        let path = self
            .server
            .env_file
            .clone()
            .unwrap_or_else(|| self.source.with_file_name(default_file));
        let bytes = match parse::read_regular(&path, true) {
            Ok(bytes) => bytes,
            Err(error)
                if self.server.env_file.is_none()
                    && error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
            {
                Vec::new()
            }
            Err(error) => return Err(error),
        };
        let mut values = BTreeMap::new();
        for item in dotenvy::from_read_iter(bytes.as_slice()) {
            // Parser diagnostics may include the secret source line.
            let (key, value) = item.map_err(|_| "invalid secrets-file assignment or quoting")?;
            if values.insert(key.clone(), value).is_some() {
                return Err(format!("duplicate secrets-file key {key}").into());
            }
        }
        // Service environment overrides the same named file setting, as before.
        for (key, value) in &mut values {
            if let Ok(environment) = std::env::var(key) {
                *value = environment;
            }
        }
        for name in [
            "ACCESS_TOKEN_DEFAULT_TTL_SECS",
            "ACCESS_TOKEN_MAX_TTL_SECS",
            "REFRESH_TOKEN_TTL_SECS",
            "SIGNED_READ_URL_SECRET",
        ] {
            if let Ok(value) = std::env::var(name) {
                values.insert(name.into(), value);
            }
        }
        for name in ["JWT_SECRET", "AUTH_KEY", "MASTER_KEY", "ADMIN_KEY"] {
            let value = std::env::var(name)
                .ok()
                .or_else(|| values.get(name).cloned())
                .ok_or_else(|| {
                    format!("{name} is required in the secrets file or service environment")
                })?;
            if value.is_empty() {
                return Err(format!("{name} must not be empty").into());
            }
            values.insert(name.into(), value);
        }
        Ok(values)
    }
}
