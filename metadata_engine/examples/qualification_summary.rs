//! Read-only summary of retained qualification evidence; never a pass oracle.
use serde_json::{Value, json};
use std::{collections::BTreeMap, io::BufRead, path::Path};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn lines(path: &Path, mut visit: impl FnMut(Value) -> Result<()>) -> Result<()> {
    for line in std::io::BufReader::new(std::fs::File::open(path)?).lines() {
        visit(serde_json::from_str(&line?)?)?;
    }
    Ok(())
}
fn distribution(mut values: Vec<f64>) -> Value {
    if values.is_empty() {
        return json!({"samples":0});
    }
    values.sort_unstable_by(f64::total_cmp);
    let quantile = |p: f64| values[((values.len() as f64 * p).ceil() as usize).saturating_sub(1)];
    json!({"samples":values.len(),"minimum":values[0],"mean":values.iter().sum::<f64>()/values.len() as f64,"p50":quantile(0.5),"p95":quantile(0.95),"p99":quantile(0.99),"maximum":values[values.len()-1]})
}
#[derive(Default)]
struct Process {
    role: String,
    peak_rss_kib: u64,
    user_ticks: u64,
    system_ticks: u64,
    samples: u64,
}
fn run(root: &Path, mode: &str) -> Result<Value> {
    if !matches!(mode, "api" | "console") {
        return Err("mode must be api or console".into());
    }
    let terminal = root.join(format!("{mode}-PASSED.json"));
    let passed = if terminal.is_file() {
        serde_json::from_slice(&std::fs::read(terminal)?)?
    } else {
        Value::Null
    };
    let mut latency = Vec::new();
    let mut batch_sizes = Vec::new();
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(&format!("{mode}-latency-")) && name.ends_with(".jsonl") {
            lines(&entry.path(), |row| {
                let millis = row["milliseconds"].as_f64().ok_or("latency missing")?;
                let count = row["count"].as_u64().ok_or("batch count missing")?;
                if !millis.is_finite() || millis < 0.0 || count == 0 {
                    return Err("invalid latency observation".into());
                }
                latency.push(millis);
                batch_sizes.push(count as f64);
                Ok(())
            })?;
        }
    }
    let mut processes = BTreeMap::<u64, Process>::new();
    let mut combined_rss = Vec::new();
    let mut samples = 0;
    let mut first_unix = None;
    let mut last_unix = None;
    lines(&root.join(format!("{mode}-resources.jsonl")), |row| {
        let unix = row["unix"].as_u64().ok_or("resource timestamp missing")?;
        first_unix.get_or_insert(unix);
        last_unix = Some(unix);
        let mut total = 0;
        for p in row["processes"]
            .as_array()
            .ok_or("process samples missing")?
        {
            let pid = p["pid"].as_u64().ok_or("PID missing")?;
            let entry = processes.entry(pid).or_default();
            entry.role = p["role"].as_str().ok_or("process role missing")?.into();
            let rss = p["rss_kib"].as_u64().ok_or("process RSS missing")?;
            entry.peak_rss_kib = entry.peak_rss_kib.max(rss);
            entry.samples += 1;
            total += rss;
            if let Some(stat) = p["stat"]
                .as_str()
                .and_then(|s| s.rsplit_once(") ").map(|r| r.1))
            {
                let fields = stat.split_whitespace().collect::<Vec<_>>();
                if fields.len() > 12 {
                    entry.user_ticks = entry.user_ticks.max(fields[11].parse()?);
                    entry.system_ticks = entry.system_ticks.max(fields[12].parse()?);
                }
            }
        }
        samples += 1;
        combined_rss.push(total as f64);
        Ok(())
    })?;
    // SAFETY: sysconf has no pointer arguments or ownership requirements.
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if ticks <= 0 {
        return Err("could not read Linux clock tick frequency".into());
    }
    let process_rows=processes.iter().map(|(pid,p)| json!({"pid":pid,"role":p.role,"samples":p.samples,"maximum_sampled_rss_kib":p.peak_rss_kib,"observed_user_seconds":p.user_ticks as f64/ticks as f64,"observed_system_seconds":p.system_ticks as f64/ticks as f64})).collect::<Vec<_>>();
    let mut browser_heap = Vec::new();
    let mut retained = Vec::new();
    let browser_path = root.join("console-browser-resources.jsonl");
    if mode == "console" && browser_path.is_file() {
        lines(&browser_path, |row| {
            if let Some(bytes) = row["heap"]["usedSize"].as_u64() {
                browser_heap.push(bytes as f64);
            }
            if let Some(items) = row["state"]["stats"]["retainedItems"].as_u64() {
                retained.push(items as f64);
            }
            Ok(())
        })?;
    }
    Ok(
        json!({"scope":"Read-only evidence summary; does not create or replace a campaign pass","terminal_report":passed,"http_batch_latency_milliseconds":distribution(latency),"objects_per_http_batch":distribution(batch_sizes),"resources":{"samples":samples,"first_unix":first_unix,"last_unix":last_unix,"combined_sampled_rss_kib":distribution(combined_rss),"processes":process_rows,"limits":"One-second snapshots can miss peaks and final CPU work before exit. Combined RSS covers object, metadata and Rust supervisor only; shared mappings can be counted more than once. CPU values are sampled cumulative counters, not GNU-time totals."},"browser":{"javascript_used_heap_bytes":distribution(browser_heap),"retained_upload_items":distribution(retained),"limits":"JavaScript heap is not browser process RSS; Chromium/native allocations are outside it."}}),
    )
}
fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 3 {
        return Err("usage: qualification_summary <campaign-root> <api|console>".into());
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&run(Path::new(&args[1]), &args[2])?)?
    );
    Ok(())
}
