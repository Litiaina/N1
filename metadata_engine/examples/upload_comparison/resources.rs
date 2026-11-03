use cli_core::Result;
use serde_json::{Value, json};
use std::{
    fs,
    io::Write,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

fn process(pid: u32) -> Result<Value> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let fields = stat[stat.rfind(')').ok_or("invalid stat")? + 1..]
        .split_whitespace()
        .collect::<Vec<_>>();
    let status = fs::read_to_string(format!("/proc/{pid}/status"))?;
    let rss = status
        .lines()
        .find_map(|l| {
            l.strip_prefix("VmRSS:")?
                .split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()
        })
        .unwrap_or(0);
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
    let io = fs::read_to_string(format!("/proc/{pid}/io"))?;
    let bytes = |field: &str| {
        io.lines()
            .find_map(|l| l.strip_prefix(field)?.trim().parse::<u64>().ok())
            .unwrap_or(0)
    };
    Ok(
        json!({"pid":pid,"rss_kib":rss,"user_seconds":fields[11].parse::<u64>()? as f64/ticks,"system_seconds":fields[12].parse::<u64>()? as f64/ticks,
        "read_bytes":bytes("read_bytes:"),"write_bytes":bytes("write_bytes:"),"rchar":bytes("rchar:"),"wchar":bytes("wchar:")}),
    )
}
fn disks() -> Result<Value> {
    let text = fs::read_to_string("/proc/diskstats")?;
    let mut out = serde_json::Map::new();
    for line in text.lines() {
        let p = line.split_whitespace().collect::<Vec<_>>();
        if !["sda", "sdc", "sdd"].contains(&p[2]) {
            continue;
        }
        let n = |i: usize| p[i].parse::<u64>().unwrap_or(0);
        out.insert(p[2].into(),json!({"reads":n(3),"read_bytes":n(5)*512,"read_ms":n(6),"writes":n(7),"write_bytes":n(9)*512,"write_ms":n(10),"busy_ms":n(12),"weighted_ms":n(13),"flushes":p.get(18).and_then(|s|s.parse::<u64>().ok()).unwrap_or(0),"flush_ms":p.get(19).and_then(|s|s.parse::<u64>().ok()).unwrap_or(0)}));
    }
    Ok(out.into())
}
pub struct Watch {
    before: Value,
    pids: [u32; 3],
    stop: Arc<AtomicBool>,
    sample: tokio::task::JoinHandle<Result<[u64; 3]>>,
    start: Instant,
}
impl Watch {
    pub fn start(object: u32, metadata: u32, path: &Path) -> Result<Self> {
        let pids = [object, metadata, std::process::id()];
        let before = json!({"processes":pids.iter().map(|&p|process(p)).collect::<Result<Vec<_>>>()?,"disks":disks()?});
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let mut file = std::io::BufWriter::new(fs::File::create(path.join("resources.jsonl"))?);
        let start = Instant::now();
        let sample = tokio::spawn(async move {
            let mut peak = [0; 3];
            while !stopping.load(Ordering::Relaxed) {
                let values = pids
                    .iter()
                    .map(|&p| process(p))
                    .collect::<Result<Vec<_>>>()?;
                for (i, p) in values.iter().enumerate() {
                    peak[i] = peak[i].max(p["rss_kib"].as_u64().unwrap_or(0));
                }
                serde_json::to_writer(
                    &mut file,
                    &json!({"seconds":start.elapsed().as_secs_f64(),"processes":values,"disks":disks()?,"io_pressure":fs::read_to_string("/proc/pressure/io")?}),
                )?;
                file.write_all(b"\n")?;
                file.flush()?;
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Ok(peak)
        });
        Ok(Self {
            before,
            pids,
            stop,
            sample,
            start,
        })
    }
    pub async fn finish(self) -> Result<Value> {
        let seconds = self.start.elapsed().as_secs_f64();
        let after = json!({"processes":self.pids.iter().map(|&p|process(p)).collect::<Result<Vec<_>>>()?,"disks":disks()?});
        self.stop.store(true, Ordering::Relaxed);
        let peak = self.sample.await??;
        let roles = ["object", "metadata", "client"];
        let mut processes = Vec::new();
        for (i, role) in roles.iter().enumerate() {
            let b = &self.before["processes"][i];
            let a = &after["processes"][i];
            let delta =
                |field: &str| a[field].as_f64().unwrap_or(0.0) - b[field].as_f64().unwrap_or(0.0);
            processes.push(json!({"role":role,"peak_sampled_rss_kib":peak[i].max(a["rss_kib"].as_u64().unwrap_or(0)),"user_seconds":delta("user_seconds"),"system_seconds":delta("system_seconds"),
                "cpu_percent_one_core":100.0*(delta("user_seconds")+delta("system_seconds"))/seconds,"read_bytes":delta("read_bytes"),"write_bytes":delta("write_bytes"),"logical_read_bytes":delta("rchar"),"logical_write_bytes":delta("wchar")}));
        }
        let mut disk = serde_json::Map::new();
        for device in ["sda", "sdc", "sdd"] {
            let b = &self.before["disks"][device];
            let a = &after["disks"][device];
            let delta =
                |field: &str| a[field].as_f64().unwrap_or(0.0) - b[field].as_f64().unwrap_or(0.0);
            disk.insert(device.into(),json!({"write_mib_per_second":delta("write_bytes")/1048576.0/seconds,"read_mib_per_second":delta("read_bytes")/1048576.0/seconds,
                "write_bytes":delta("write_bytes"),"read_bytes":delta("read_bytes"),"busy_percent":delta("busy_ms")/10.0/seconds,
                "write_await_ms":delta("write_ms")/delta("writes").max(1.0),"read_await_ms":delta("read_ms")/delta("reads").max(1.0),"flushes":delta("flushes"),"flush_ms":delta("flush_ms")}));
        }
        Ok(json!({"processes":processes,"disks":disk,"seconds":seconds}))
    }
}
