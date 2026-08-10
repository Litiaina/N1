use crate::io_control::latency::{IoLatencySnapshot, io_latency_snapshot};
use crate::io_control::resource::{ProcessResourceSnapshot, process_resource_snapshot};
use crate::io_control::{IoControlConfig, io_control_config};
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct IoControlSnapshot {
    pub config: IoControlConfig,
    pub latency: IoLatencySnapshot,
    pub process: ProcessResourceSnapshot,
}

pub fn io_control_snapshot() -> IoControlSnapshot {
    IoControlSnapshot {
        config: io_control_config(),
        latency: io_latency_snapshot(),
        process: process_resource_snapshot(),
    }
}
