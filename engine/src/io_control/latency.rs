use once_cell::sync::Lazy;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static LATENCIES: Lazy<Mutex<HashMap<String, LatencyState>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static SLOW_EVENTS: Lazy<Mutex<VecDeque<SlowPathEvent>>> =
    Lazy::new(|| Mutex::new(VecDeque::new()));

const DEFAULT_MAX_SAMPLES: usize = 1024;
const DEFAULT_SLOW_PATH_MS: u64 = 1000;
const MAX_SLOW_EVENTS: usize = 64;

#[derive(Clone, Debug, Default)]
struct LatencyState {
    samples_ms: VecDeque<u64>,
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
    pub min_ms: u64,
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub p99_ms: u64,
    pub max_ms: u64,
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

fn max_samples() -> usize {
    std::env::var("N2_IO_LATENCY_SAMPLES")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_SAMPLES)
        .clamp(128, 65_536)
}

fn slow_path_threshold_ms() -> u64 {
    std::env::var("N2_SLOW_IO_MS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_SLOW_PATH_MS)
        .clamp(10, 300_000)
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

pub fn record_io_latency(label: impl Into<String>, elapsed: Duration, bytes: u64, success: bool) {
    let label = label.into();
    let duration_ms = elapsed.as_millis() as u64;
    let max_samples = max_samples();
    if let Ok(mut latencies) = LATENCIES.lock() {
        let state = latencies.entry(label.clone()).or_default();
        state.observed = state.observed.saturating_add(1);
        if !success {
            state.failures = state.failures.saturating_add(1);
        }
        state.bytes = state.bytes.saturating_add(bytes);
        state.samples_ms.push_back(duration_ms);
        while state.samples_ms.len() > max_samples {
            state.samples_ms.pop_front();
        }
    }
    if duration_ms >= slow_path_threshold_ms()
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
            let mut samples = state.samples_ms.iter().copied().collect::<Vec<_>>();
            samples.sort_unstable();
            classes.push(LatencyClassSnapshot {
                label: label.clone(),
                samples_retained: samples.len(),
                observed: state.observed,
                failures: state.failures,
                bytes: state.bytes,
                min_ms: samples.first().copied().unwrap_or(0),
                p50_ms: percentile(&samples, 0.50),
                p95_ms: percentile(&samples, 0.95),
                p99_ms: percentile(&samples, 0.99),
                max_ms: samples.last().copied().unwrap_or(0),
            });
        }
    }
    classes.sort_by(|a, b| a.label.cmp(&b.label));
    let recent_slow_paths = SLOW_EVENTS
        .lock()
        .map(|events| events.iter().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    IoLatencySnapshot {
        max_samples_per_class: max_samples(),
        slow_path_threshold_ms: slow_path_threshold_ms(),
        classes,
        recent_slow_paths,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_snapshot_reports_percentiles() {
        record_io_latency("test.percentile", Duration::from_millis(10), 1, true);
        record_io_latency("test.percentile", Duration::from_millis(20), 1, true);
        let snap = io_latency_snapshot();
        let class = snap
            .classes
            .iter()
            .find(|class| class.label == "test.percentile")
            .expect("latency class");
        assert!(class.p50_ms >= 10);
        assert!(class.p95_ms >= class.p50_ms);
    }
}
