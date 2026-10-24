use crate::core::linux_io::{BackendSnapshot, backend_snapshot};
use crate::io_control::latency::{IoLatencySnapshot, io_latency_snapshot};
use crate::io_control::resource::{ProcessResourceSnapshot, process_resource_snapshot};
use crate::io_control::{IoControlConfig, io_control_config};
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct IoControlSnapshot {
    pub backend: BackendSnapshot,
    pub config: IoControlConfig,
    pub latency: IoLatencySnapshot,
    pub process: ProcessResourceSnapshot,
}

pub fn io_control_snapshot() -> IoControlSnapshot {
    IoControlSnapshot {
        backend: backend_snapshot(),
        config: io_control_config(),
        latency: io_latency_snapshot(),
        process: process_resource_snapshot(),
    }
}

#[cfg(test)]
mod tests {
    use super::io_control_snapshot;

    #[test]
    fn health_snapshot_exposes_backend_selection_and_operation_counts() {
        let value = serde_json::to_value(io_control_snapshot()).expect("serialize I/O health");
        let backend = &value["backend"];
        assert!(backend["backend"].is_string());
        assert_eq!(backend["publication_backend"], "posix");
        assert!(backend["requested"].is_string());
        assert!(backend["fallback_reason"].is_null() || backend["fallback_reason"].is_string());
        for field in [
            "queue_capacity",
            "queued",
            "in_flight",
            "submitted",
            "completed",
        ] {
            assert!(backend[field].is_u64(), "missing backend counter {field}");
        }
    }
}
