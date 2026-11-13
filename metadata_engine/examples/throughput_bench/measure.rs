use super::*;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};
pub type Pids = Vec<(&'static str, u32)>;
fn process(pid: u32) -> Result<Value> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let fields: Vec<_> = stat
        .rsplit_once(')')
        .ok_or("invalid process stat")?
        .1
        .split_whitespace()
        .collect();
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
    let status = fs::read_to_string(format!("/proc/{pid}/status"))?;
    let kib = |name: &str| -> Result<u64> {
        Ok(status
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .ok_or("missing memory counter")?
            .split_whitespace()
            .next()
            .ok_or("empty memory counter")?
            .parse()?)
    };
    let io = fs::read_to_string(format!("/proc/{pid}/io"))?;
    let counter = |name: &str| -> Result<u64> {
        Ok(io
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .ok_or("missing I/O counter")?
            .trim()
            .parse()?)
    };
    let user = fields[11].parse::<u64>()? as f64 / ticks;
    let system = fields[12].parse::<u64>()? as f64 / ticks;
    Ok(
        json!({"pid":pid,"start_ticks":fields[19],"cpu_seconds":user+system,"user_seconds":user,"system_seconds":system,
        "rss_kib":kib("VmRSS:")?,"lifetime_peak_rss_kib":kib("VmHWM:")?,"swap_kib":kib("VmSwap:")?,
        "read_bytes":counter("read_bytes:")?,"write_bytes":counter("write_bytes:")?}),
    )
}
pub fn distribution(mut values: Vec<f64>) -> Value {
    let order = values.clone();
    values.sort_by(f64::total_cmp);
    let p = |q: f64| values[((values.len() as f64 * q).ceil() as usize).saturating_sub(1)];
    json!({"samples":values.len(),"p50_ms":p(0.5),"p95_ms":p(0.95),"p99_ms":p(0.99),"max_ms":values.last(),"completion_order_ms":order})
}
pub struct Measure {
    started: Instant,
    before: BTreeMap<&'static str, Value>,
    stop: Arc<AtomicBool>,
    sampler: tokio::task::JoinHandle<Result<BTreeMap<&'static str, u64>>>,
}
impl Measure {
    pub fn start(pids: &Pids) -> Result<Self> {
        let before = pids
            .iter()
            .map(|(name, pid)| Ok((*name, process(*pid)?)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let pids = pids.clone();
        let sampler = tokio::spawn(async move {
            let mut peak = BTreeMap::new();
            while !stopping.load(Ordering::Relaxed) {
                for (name, pid) in &pids {
                    let rss = process(*pid)?["rss_kib"].as_u64().ok_or("missing RSS")?;
                    let old = peak.entry(*name).or_insert(0);
                    *old = (*old).max(rss);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok(peak)
        });
        Ok(Self {
            started: Instant::now(),
            before,
            stop,
            sampler,
        })
    }
    pub async fn finish(self, bytes: u64, operations: usize) -> Result<Value> {
        let seconds = self.started.elapsed().as_secs_f64();
        self.stop.store(true, Ordering::Relaxed);
        let peak = self.sampler.await??;
        let mut processes = BTreeMap::new();
        for (name, before) in self.before {
            let after = process(before["pid"].as_u64().ok_or("missing PID")? as u32)?;
            if before["start_ticks"] != after["start_ticks"] {
                return Err("service restarted during measurement".into());
            }
            let cpu =
                after["cpu_seconds"].as_f64().unwrap() - before["cpu_seconds"].as_f64().unwrap();
            processes.insert(name,json!({"cpu_seconds":cpu,"cpu_percent_of_one_core":cpu/seconds*100.0,
                "user_seconds":after["user_seconds"].as_f64().unwrap()-before["user_seconds"].as_f64().unwrap(),
                "system_seconds":after["system_seconds"].as_f64().unwrap()-before["system_seconds"].as_f64().unwrap(),
                "sampled_peak_rss_kib":peak.get(name).copied().unwrap_or(0).max(before["rss_kib"].as_u64().unwrap()).max(after["rss_kib"].as_u64().unwrap()),
                "read_bytes":after["read_bytes"].as_u64().unwrap().saturating_sub(before["read_bytes"].as_u64().unwrap()),
                "write_bytes":after["write_bytes"].as_u64().unwrap().saturating_sub(before["write_bytes"].as_u64().unwrap()),"before":before,"after":after}));
        }
        Ok(
            json!({"wall_seconds":seconds,"source_bytes":bytes,"operations":operations,"mib_per_second":bytes as f64/1048576.0/seconds,"operations_per_second":operations as f64/seconds,"processes":processes}),
        )
    }
}
