use super::{Result, api};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    future::Future,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Serialize)]
pub struct Sample {
    pub role: &'static str,
    pub operation: &'static str,
    pub start_ms: f64,
    pub latency_ms: f64,
    pub bytes: u64,
    pub error: Option<String>,
}
pub struct Recorder {
    start: Instant,
    samples: Mutex<Vec<Sample>>,
    active: AtomicUsize,
    roles: [AtomicUsize; 5],
    peak_active: AtomicUsize,
    peak_roles: AtomicUsize,
}
impl Recorder {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            start: Instant::now(),
            samples: Mutex::new(Vec::new()),
            active: AtomicUsize::new(0),
            roles: std::array::from_fn(|_| AtomicUsize::new(0)),
            peak_active: AtomicUsize::new(0),
            peak_roles: AtomicUsize::new(0),
        })
    }
    pub async fn call<T>(
        &self,
        role: &'static str,
        operation: &'static str,
        bytes: u64,
        future: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        let index = match role {
            "bulk" => 0,
            "video" => 1,
            "download" => 2,
            "interactive" => 3,
            "delete" => 4,
            _ => return Err("unknown workload role".into()),
        };
        self.roles[index].fetch_add(1, Ordering::Relaxed);
        let active = self.active.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak_active.fetch_max(active, Ordering::Relaxed);
        self.peak_roles.fetch_max(
            self.roles
                .iter()
                .filter(|r| r.load(Ordering::Relaxed) > 0)
                .count(),
            Ordering::Relaxed,
        );
        let start = Instant::now();
        let start_ms = self.start.elapsed().as_secs_f64() * 1000.0;
        let result = future.await;
        self.roles[index].fetch_sub(1, Ordering::Relaxed);
        self.active.fetch_sub(1, Ordering::Relaxed);
        let mut samples = self
            .samples
            .lock()
            .map_err(|_| "workload recorder poisoned")?;
        if samples.len() >= 250_000 {
            return Err("bounded workload sample capacity exceeded".into());
        }
        samples.push(Sample {
            role,
            operation,
            start_ms,
            latency_ms: start.elapsed().as_secs_f64() * 1000.0,
            bytes,
            error: result.as_ref().err().map(ToString::to_string),
        });
        result
    }
    pub fn summary(
        &self,
        output: &Path,
        label: &str,
        seconds: f64,
        offered_seconds: f64,
    ) -> Result<Value> {
        let samples = self
            .samples
            .lock()
            .map_err(|_| "workload recorder poisoned")?;
        api::save(
            &output.join(format!("{label}-requests.json")),
            &serde_json::to_value(&*samples)?,
        )?;
        let mut groups: BTreeMap<&str, Vec<&Sample>> = BTreeMap::new();
        for sample in samples.iter() {
            groups.entry(sample.operation).or_default().push(sample);
        }
        let summarize = |rows: &[&Sample], duration: f64| {
            let mut row = distribution(rows.iter().map(|r| r.latency_ms).collect());
            let bytes: u64 = rows
                .iter()
                .filter(|r| r.error.is_none())
                .map(|r| r.bytes)
                .sum();
            row["bytes"] = json!(bytes);
            row["mib_per_second"] = json!(bytes as f64 / 1048576.0 / duration);
            row["operations_per_second"] = json!(rows.len() as f64 / duration);
            row["errors"] = json!(rows.iter().filter(|r| r.error.is_some()).count());
            row
        };
        let operations = groups
            .iter()
            .map(|(operation, rows)| (*operation, summarize(rows, seconds)))
            .collect::<BTreeMap<_, _>>();
        let offered_window = groups
            .iter()
            .map(|(operation, rows)| {
                // Exclude late completions and the large-object drain from sustained-window rates.
                let rows = rows
                    .iter()
                    .copied()
                    .filter(|r| r.start_ms + r.latency_ms <= offered_seconds * 1000.0)
                    .collect::<Vec<_>>();
                (*operation, summarize(&rows, offered_seconds))
            })
            .collect::<BTreeMap<_, _>>();
        let errors = samples.iter().filter(|s| s.error.is_some()).count();
        Ok(
            json!({"operations":operations,"offered_window_operations":offered_window,"request_samples":samples.len(),"errors":errors,
            "peak_requests_in_flight":self.peak_active.load(Ordering::Relaxed),
            "peak_distinct_roles_in_flight":self.peak_roles.load(Ordering::Relaxed)}),
        )
    }
    pub fn progress(&self) -> Value {
        let samples = self.samples.lock().unwrap();
        json!({"completed_requests":samples.len(),"active_requests":self.active.load(Ordering::Relaxed),
            "completed_payload_bytes":samples.iter().map(|s|s.bytes).sum::<u64>()})
    }
}
pub fn distribution(mut values: Vec<f64>) -> Value {
    if values.is_empty() {
        return json!({"samples":0});
    }
    values.sort_by(f64::total_cmp);
    let percentile = |q: f64| values[((values.len() as f64 * q).ceil() as usize).saturating_sub(1)];
    json!({"samples":values.len(),"p50_ms":percentile(0.50),"p95_ms":percentile(0.95),
        "p99_ms":percentile(0.99),"maximum_ms":values.last()})
}

fn process(pid: u32) -> Result<Value> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let fields = stat
        .rsplit_once(')')
        .ok_or("invalid process stat")?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
    let status = fs::read_to_string(format!("/proc/{pid}/status"))?;
    let io = fs::read_to_string(format!("/proc/{pid}/io"))?;
    let counter = |content: &str, name: &str| -> Result<u64> {
        Ok(content
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .ok_or("missing process counter")?
            .split_whitespace()
            .next()
            .ok_or("empty process counter")?
            .parse()?)
    };
    Ok(
        json!({"start_ticks":fields[19],"cpu_seconds":(fields[11].parse::<u64>()?+fields[12].parse::<u64>()?) as f64/ticks,
        "rss_kib":counter(&status,"VmRSS:")?,"hwm_kib":counter(&status,"VmHWM:")?,"swap_kib":counter(&status,"VmSwap:")?,
        "read_bytes":counter(&io,"read_bytes:")?,"write_bytes":counter(&io,"write_bytes:")?}),
    )
}
fn disks() -> Result<Value> {
    let mut disks = BTreeMap::new();
    for line in fs::read_to_string("/proc/diskstats")?.lines() {
        let f = line.split_whitespace().collect::<Vec<_>>();
        if f.len() < 14 || !f[2].starts_with("sd") || !f[2].chars().all(|c| c.is_ascii_alphabetic())
        {
            continue;
        }
        let n = |index: usize| -> Result<u64> { Ok(f[index].parse()?) };
        disks.insert(f[2].to_owned(),json!({"reads":n(3)?,"read_sectors":n(5)?,"read_ms":n(6)?,
            "writes":n(7)?,"write_sectors":n(9)?,"write_ms":n(10)?,"in_flight":n(11)?,"busy_ms":n(12)?,"weighted_ms":n(13)?}));
    }
    Ok(json!(disks))
}
pub struct Monitor {
    started: Instant,
    pid: u32,
    before: Value,
    client_before: Value,
    disks_before: Value,
    stop: Arc<AtomicBool>,
    sampler: tokio::task::JoinHandle<Result<Vec<Value>>>,
}
impl Monitor {
    pub fn start(pid: u32) -> Result<Self> {
        let before = process(pid)?;
        let client_before = process(std::process::id())?;
        let disks_before = disks()?;
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let started = Instant::now();
        let sampler = tokio::spawn(async move {
            let mut rows = Vec::new();
            while !stopping.load(Ordering::Relaxed) {
                rows.push(
                    json!({"seconds":started.elapsed().as_secs_f64(),"n1":process(pid)?,
                    "client":process(std::process::id())?,"disks":disks()?,
                    "loadavg":fs::read_to_string("/proc/loadavg")?.trim()}),
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Ok(rows)
        });
        Ok(Self {
            started,
            pid,
            before,
            client_before,
            disks_before,
            stop,
            sampler,
        })
    }
    pub async fn finish(self, output: &Path, label: &str) -> Result<Value> {
        let seconds = self.started.elapsed().as_secs_f64();
        self.stop.store(true, Ordering::Relaxed);
        let rows = self.sampler.await??;
        let after = process(self.pid)?;
        let client_after = process(std::process::id())?;
        if self.before["start_ticks"] != after["start_ticks"] {
            return Err("N1 restarted within timed workload".into());
        }
        api::save(
            &output.join(format!("{label}-resources.json")),
            &json!(rows),
        )?;
        let mut processes = BTreeMap::new();
        for (name, before, after) in [
            ("n1", self.before, after),
            ("client", self.client_before, client_after),
        ] {
            let cpu =
                after["cpu_seconds"].as_f64().unwrap() - before["cpu_seconds"].as_f64().unwrap();
            let peak = rows
                .iter()
                .filter_map(|r| r[name]["rss_kib"].as_u64())
                .max()
                .unwrap_or(0);
            processes.insert(
                name,
                json!({"cpu_seconds":cpu,"cpu_percent_one_core":cpu/seconds*100.0,
                "sampled_peak_rss_kib":peak,"before":before,"after":after}),
            );
        }
        Ok(
            json!({"wall_seconds":seconds,"processes":processes,"disk_before":self.disks_before,"disk_after":disks()?}),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn nearest_rank_keeps_tail_outliers_and_handles_empty_samples() {
        let mut samples = vec![1.0; 99];
        samples.push(1000.0);
        let row = distribution(samples);
        assert_eq!(row["p99_ms"], 1.0);
        assert_eq!(row["maximum_ms"], 1000.0);
        assert_eq!(distribution(Vec::new())["samples"], 0);
    }
}
