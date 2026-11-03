use crate::io_control::io_control_config;
use once_cell::sync::Lazy;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static LATENCIES: Lazy<Mutex<HashMap<String, LatencyState>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static SLOW_EVENTS: Lazy<Mutex<VecDeque<SlowPathEvent>>> =
    Lazy::new(|| Mutex::new(VecDeque::new()));

const MAX_SLOW_EVENTS: usize = 64;

#[derive(Clone, Debug, Default)]
struct LatencyState {
    samples_us: VecDeque<u64>,
    observed: u64,
    failures: u64,
    bytes: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct LatencyClassSnapshot {
    pub label: String,
    pub samples_retained: usize,
    pub observed: u64,
    pub failures: u64,
    pub bytes: u64,
    pub min_us: u64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct SlowPathEvent {
    pub at_unix: i64,
    pub label: String,
    pub duration_ms: u64,
    pub bytes: u64,
    pub success: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct IoLatencySnapshot {
    pub max_samples_per_class: usize,
    pub slow_path_threshold_ms: u64,
    pub classes: Vec<LatencyClassSnapshot>,
    pub recent_slow_paths: Vec<SlowPathEvent>,
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

pub fn record_io_latency(label: impl Into<String>, elapsed: Duration, bytes: u64, success: bool) {
    let label = label.into();
    let duration_us = elapsed.as_micros().min(u64::MAX as u128) as u64;
    let config = io_control_config();
    if let Ok(mut latencies) = LATENCIES.lock() {
        let state = latencies.entry(label.clone()).or_default();
        state.observed = state.observed.saturating_add(1);
        if !success {
            state.failures = state.failures.saturating_add(1);
        }
        state.bytes = state.bytes.saturating_add(bytes);
        state.samples_us.push_back(duration_us);
        while state.samples_us.len() > config.latency_samples_per_class {
            state.samples_us.pop_front();
        }
    }

    let duration_ms = elapsed.as_millis().min(u64::MAX as u128) as u64;
    if duration_ms >= config.slow_io_threshold_ms
        && let Ok(mut slow) = SLOW_EVENTS.lock()
    {
        slow.push_back(SlowPathEvent {
            at_unix: now_unix(),
            label,
            duration_ms,
            bytes,
            success,
        });
        while slow.len() > MAX_SLOW_EVENTS {
            slow.pop_front();
        }
    }
}

fn percentile(sorted: &[u64], percentile: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((sorted.len().saturating_sub(1)) as f64 * percentile).round() as usize;
    sorted[rank.min(sorted.len().saturating_sub(1))]
}

pub fn io_latency_snapshot() -> IoLatencySnapshot {
    let mut classes = Vec::new();
    if let Ok(latencies) = LATENCIES.lock() {
        for (label, state) in latencies.iter() {
            let mut samples = state.samples_us.iter().copied().collect::<Vec<_>>();
            samples.sort_unstable();
            classes.push(LatencyClassSnapshot {
                label: label.clone(),
                samples_retained: samples.len(),
                observed: state.observed,
                failures: state.failures,
                bytes: state.bytes,
                min_us: samples.first().copied().unwrap_or(0),
                p50_us: percentile(&samples, 0.50),
                p95_us: percentile(&samples, 0.95),
                p99_us: percentile(&samples, 0.99),
                max_us: samples.last().copied().unwrap_or(0),
            });
        }
    }
    classes.sort_by(|a, b| a.label.cmp(&b.label));
    let recent_slow_paths = SLOW_EVENTS
        .lock()
        .map(|events| events.iter().cloned().collect())
        .unwrap_or_default();
    let config = io_control_config();
    IoLatencySnapshot {
        max_samples_per_class: config.latency_samples_per_class,
        slow_path_threshold_ms: config.slow_io_threshold_ms,
        classes,
        recent_slow_paths,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_snapshot_reports_microsecond_percentiles() {
        let label = format!("test.percentile.{}", std::process::id());
        record_io_latency(&label, Duration::from_micros(10), 1, true);
        record_io_latency(&label, Duration::from_micros(20), 1, true);
        let snapshot = io_latency_snapshot();
        let class = snapshot
            .classes
            .iter()
            .find(|class| class.label == label)
            .expect("latency class");
        assert!(class.p50_us >= 10);
        assert!(class.p95_us >= class.p50_us);
    }
}
