//! Bounded request/resource observations; no server instrumentation is needed.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(serde::Serialize)]
struct Sample {
    operation: &'static str,
    start_ms: f64,
    latency_ms: f64,
    bytes: u64,
    completed: bool,
}

pub(super) struct Meter {
    start: Instant,
    active: [AtomicUsize; 8],
    peak: AtomicUsize,
    overlaps: AtomicUsize,
    samples: Mutex<Vec<Sample>>,
    observations: Mutex<Vec<Value>>,
}

impl Meter {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            start: Instant::now(),
            active: std::array::from_fn(|_| AtomicUsize::new(0)),
            peak: AtomicUsize::new(0),
            overlaps: AtomicUsize::new(0),
            samples: Mutex::new(Vec::new()),
            observations: Mutex::new(Vec::new()),
        })
    }

    pub fn observation(&self, value: Value) {
        let mut observations = self.observations.lock().unwrap();
        assert!(
            observations.len() < 5_000,
            "bounded observation capacity exceeded"
        );
        observations.push(value);
    }

    pub async fn call<T>(
        self: &Arc<Self>,
        role: usize,
        operation: &'static str,
        bytes: u64,
        future: impl std::future::Future<Output = T>,
    ) -> T {
        self.active[role].fetch_add(1, Ordering::Relaxed);
        let counts = self.active.each_ref().map(|v| v.load(Ordering::Relaxed));
        self.peak.fetch_max(counts.iter().sum(), Ordering::Relaxed);
        if counts[..5].iter().sum::<usize>() > 0 && counts[5..].iter().sum::<usize>() > 0 {
            self.overlaps.fetch_add(1, Ordering::Relaxed);
        }
        let mut guard = Call {
            meter: self.clone(),
            role,
            operation,
            bytes,
            start: Instant::now(),
            completed: false,
        };
        let value = future.await;
        guard.completed = true;
        value
    }

    pub fn finish(&self, root: &Path, name: &str, offered: u64) -> Value {
        let samples = self.samples.lock().unwrap();
        reliability::record(&root.join(format!("{name}-requests.json")), &*samples);
        let observations = self.observations.lock().unwrap();
        reliability::record(&root.join(format!("{name}-health.json")), &*observations);
        let mut groups = std::collections::BTreeMap::<_, Vec<_>>::new();
        for sample in samples.iter() {
            groups.entry(sample.operation).or_default().push(sample);
        }
        let mut operations = serde_json::Map::new();
        for (operation, rows) in groups {
            let latencies = rows.iter().map(|s| s.latency_ms).collect();
            let mut stats = preparation_bench::distribution(latencies);
            stats["errors"] = json!(rows.iter().filter(|s| !s.completed).count());
            stats["offered_window_bytes"] = json!(
                rows.iter()
                    .filter(|s| s.completed && s.start_ms + s.latency_ms <= offered as f64 * 1000.)
                    .map(|s| s.bytes)
                    .sum::<u64>()
            );
            operations.insert(operation.into(), stats);
        }
        json!({
            "operations": operations,
            "samples": samples.len(),
            "errors": samples.iter().filter(|s| !s.completed).count(),
            "peak_requests": self.peak.load(Ordering::Relaxed),
            "health_samples": observations.len(),
            "degraded_health_samples": observations.iter().filter(|v| v["status"]=="degraded").count(),
            "foreground_maintenance_overlaps": self.overlaps.load(Ordering::Relaxed)
        })
    }
}

struct Call {
    meter: Arc<Meter>,
    role: usize,
    operation: &'static str,
    bytes: u64,
    start: Instant,
    completed: bool,
}

impl Drop for Call {
    fn drop(&mut self) {
        self.meter.active[self.role].fetch_sub(1, Ordering::Relaxed);
        let mut samples = self.meter.samples.lock().unwrap();
        assert!(samples.len() < 100_000, "bounded request capacity exceeded");
        samples.push(Sample {
            operation: self.operation,
            start_ms: self.start.duration_since(self.meter.start).as_secs_f64() * 1000.,
            latency_ms: self.start.elapsed().as_secs_f64() * 1000.,
            bytes: self.bytes,
            completed: self.completed,
        });
    }
}
