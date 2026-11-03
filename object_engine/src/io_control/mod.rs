pub mod latency;
pub mod metrics;
pub mod resource;
pub mod stall;

use once_cell::sync::OnceCell;
use serde::Serialize;
use std::time::Duration;

static IO_CONTROL_CONFIG: OnceCell<IoControlConfig> = OnceCell::new();

#[derive(Clone, Copy, Debug, Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IoControlConfig {
    pub foreground_wait_timeout_ms: u64,
    pub stall_threshold_ms: u64,
    pub slow_io_threshold_ms: u64,
    pub latency_samples_per_class: usize,
}

impl Default for IoControlConfig {
    fn default() -> Self {
        Self {
            foreground_wait_timeout_ms: 30_000,
            stall_threshold_ms: 20_000,
            slow_io_threshold_ms: 1_000,
            latency_samples_per_class: 1_024,
        }
    }
}

impl IoControlConfig {
    pub fn normalized(self) -> Self {
        Self {
            foreground_wait_timeout_ms: self.foreground_wait_timeout_ms.clamp(100, 300_000),
            stall_threshold_ms: self.stall_threshold_ms.clamp(1_000, 600_000),
            slow_io_threshold_ms: self.slow_io_threshold_ms.clamp(10, 300_000),
            latency_samples_per_class: self.latency_samples_per_class.clamp(128, 65_536),
        }
    }
}

pub fn configure_io_control(config: IoControlConfig) -> Result<(), &'static str> {
    IO_CONTROL_CONFIG
        .set(config.normalized())
        .map_err(|_| "N1 IO control was already configured")
}

pub fn io_control_config() -> IoControlConfig {
    IO_CONTROL_CONFIG.get().copied().unwrap_or_default()
}

pub fn foreground_wait_timeout() -> Duration {
    Duration::from_millis(io_control_config().foreground_wait_timeout_ms)
}

pub fn stall_threshold() -> Duration {
    Duration::from_millis(io_control_config().stall_threshold_ms)
}

pub use latency::{
    IoLatencySnapshot, LatencyClassSnapshot, SlowPathEvent, io_latency_snapshot, record_io_latency,
};
pub use metrics::{IoControlSnapshot, io_control_snapshot};
pub use resource::{ProcessResourceSnapshot, process_resource_snapshot};
pub use stall::{ProgressTracker, StallSnapshot};
