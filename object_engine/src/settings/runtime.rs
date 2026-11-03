//! Operator controls independent of logical metadata and immutable LINK bytes.
use crate::{
    core::{Error, Result},
    io_control::IoControlConfig,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeConfig {
    pub io_control: IoControlConfig,
    pub writer_buffer_size: usize,
    pub upload_io_gate_enabled: bool,
    pub upload_part_strict_durability_default: bool,
    pub max_part_size: u64,
    pub max_file_size: u64,
    pub stream_locality_bias_enabled: bool,
    pub staging_min_age_seconds: u64,
    pub scheduled_scrub_pause_millis: u64,
    pub object_scan_concurrency: usize,
    pub shard_repair_concurrency: usize,
    pub metadata_batch_size: usize,
    pub topology_check_interval_seconds: u64,
    pub topology_recovery_stability_seconds: u64,
    pub monitor_interval_seconds: u64,
    pub monitor_enabled: bool,
    pub monitor_deep_scan_interval_seconds: u64,
    pub login_token_expiration: u64,
    pub access_token_max_ttl_seconds: u64,
    pub refresh_token_ttl_seconds: u64,
}
impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            io_control: Default::default(),
            writer_buffer_size: 0,
            upload_io_gate_enabled: true,
            upload_part_strict_durability_default: false,
            max_part_size: n1_link::noa::MAX_BODY_BYTES,
            max_file_size: i64::MAX as u64,
            stream_locality_bias_enabled: true,
            staging_min_age_seconds: 0,
            scheduled_scrub_pause_millis: 0,
            object_scan_concurrency: 1,
            shard_repair_concurrency: 256,
            metadata_batch_size: 128,
            topology_check_interval_seconds: 30,
            topology_recovery_stability_seconds: 10,
            monitor_interval_seconds: 300,
            monitor_enabled: false,
            monitor_deep_scan_interval_seconds: 3600,
            login_token_expiration: 3600,
            access_token_max_ttl_seconds: 86400,
            refresh_token_ttl_seconds: 604800,
        }
    }
}
impl RuntimeConfig {
    pub fn validate(&self) -> Result<()> {
        let normalized = self.io_control.normalized();
        if normalized.foreground_wait_timeout_ms != self.io_control.foreground_wait_timeout_ms
            || normalized.stall_threshold_ms != self.io_control.stall_threshold_ms
            || normalized.slow_io_threshold_ms != self.io_control.slow_io_threshold_ms
            || normalized.latency_samples_per_class != self.io_control.latency_samples_per_class
            || self.writer_buffer_size > 64 * 1024 * 1024
            || self.max_part_size == 0
            || self.max_file_size == 0
            || self.max_file_size > i64::MAX as u64
            || self.scheduled_scrub_pause_millis > 60000
            || !(1..=4096).contains(&self.object_scan_concurrency)
            || !(1..=4096).contains(&self.shard_repair_concurrency)
            || !(1..=1000).contains(&self.metadata_batch_size)
            || !(1..=86400).contains(&self.login_token_expiration)
            || self.login_token_expiration > self.access_token_max_ttl_seconds
            || !(1..=86400).contains(&self.access_token_max_ttl_seconds)
            || !(1..=30 * 86400).contains(&self.refresh_token_ttl_seconds)
            || self.topology_check_interval_seconds == 0
            || self.monitor_interval_seconds == 0
            || self.monitor_deep_scan_interval_seconds == 0
        {
            return Err(Error::Invalid("invalid N1 runtime controls".into()));
        }
        Ok(())
    }
    pub fn configure(&self) -> Result<()> {
        self.validate()?;
        crate::io_control::configure_io_control(self.io_control)
            .map_err(|error| Error::Conflict(error.into()))?;
        crate::core::object_parser::STREAM_LOCALITY_BIAS_ENABLED_ATOMIC
            .set(std::sync::atomic::AtomicBool::new(
                self.stream_locality_bias_enabled,
            ))
            .map_err(|_| Error::Conflict("stream locality policy already configured".into()))?;
        Ok(())
    }
}
