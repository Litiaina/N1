//! Bounded node observations. These timers inspect topology and filesystem
//! capacity, never walk object contents or trigger implicit healing.
use super::{Store, system::SystemMetrics};
use serde::Serialize;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Default, Serialize)]
pub struct MonitorStatus {
    pub last_topology_check: Option<u64>,
    pub available_members: usize,
    pub configured_members: usize,
    pub topology_stable: bool,
    pub last_system_check: Option<u64>,
    pub system: Option<SystemMetrics>,
    pub last_capacity_check: Option<u64>,
    pub capacity: Option<serde_json::Value>,
    pub errors: Vec<String>,
}
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
pub async fn run(store: Arc<Store>, stop: CancellationToken) {
    let config = store.runtime().clone();
    if !config.monitor_enabled {
        return;
    }
    let mut topology =
        tokio::time::interval(Duration::from_secs(config.topology_check_interval_seconds));
    let mut system = tokio::time::interval(Duration::from_secs(config.monitor_interval_seconds));
    let mut capacity = tokio::time::interval(Duration::from_secs(
        config.monitor_deep_scan_interval_seconds,
    ));
    for timer in [&mut topology, &mut system, &mut capacity] {
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    }
    let mut stable_since = None;
    loop {
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = topology.tick() => {
                let engine = store.clone();
                let checked = tokio::task::spawn_blocking(move || {
                    let mut available = 0;
                    let mut configured = 0;
                    let mut errors = Vec::new();
                    for pool in &engine.pools {
                        configured += pool.layout.width();
                        for (slot, member) in pool.members.iter().enumerate() {
                            if member.as_ref().is_some_and(|member| member.check_binding().is_ok()) {
                                available += 1;
                            } else if errors.len() < 32 {
                                errors.push(format!(
                                    "pool {} member {slot} is unavailable or its binding differs",
                                    pool.layout.id
                                ));
                            }
                        }
                    }
                    (available, configured, errors)
                })
                .await;
                if let Ok((available, configured, errors)) = checked {
                    if available == configured {
                        stable_since.get_or_insert_with(Instant::now);
                    } else {
                        stable_since = None;
                    }
                    let stable = stable_since.is_some_and(|since| {
                        since.elapsed() >= Duration::from_secs(config.topology_recovery_stability_seconds)
                    });
                    let mut state = store.monitor.lock().unwrap_or_else(|error| error.into_inner());
                    state.last_topology_check = Some(now());
                    state.available_members = available;
                    state.configured_members = configured;
                    state.topology_stable = stable;
                    state.errors = errors;
                }
            }
            _ = system.tick() => {
                let metrics = super::system::get_system_metrics().await;
                let mut state = store.monitor.lock().unwrap_or_else(|error| error.into_inner());
                state.system = Some(metrics);
                state.last_system_check = Some(now());
            }
            _ = capacity.tick() => {
                let result = super::capacity_manager::get_storage_usage(&store.pools).await;
                let mut state = store.monitor.lock().unwrap_or_else(|error| error.into_inner());
                state.last_capacity_check = Some(now());
                match result {
                    Ok(value) => state.capacity = serde_json::to_value(value).ok(),
                    Err(_) => {
                        if state.errors.len() < 32 {
                            state.errors.push("capacity observation is unavailable".into());
                        }
                    }
                }
            }
        }
    }
}
