//! Read-only Linux process/device observations for an existing qualification.
//! This never calls an N1 API or changes host tuning or service state.
use clap::Parser;
use serde_json::{Value, json};
use std::{collections::BTreeMap, io::Write, os::unix::fs::OpenOptionsExt, path::PathBuf};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    root: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    owner: u32,
}
fn read(path: impl AsRef<std::path::Path>) -> Option<String> {
    std::fs::read_to_string(path).ok()
}
fn process(pid: u64) -> Value {
    let root = PathBuf::from(format!("/proc/{pid}"));
    let mut waits = BTreeMap::<String, u64>::new();
    let mut blocked = Vec::new();
    if let Ok(tasks) = std::fs::read_dir(root.join("task")) {
        for task in tasks.flatten() {
            let path = task.path();
            let wait = read(path.join("wchan"))
                .unwrap_or_default()
                .trim()
                .to_owned();
            *waits.entry(wait).or_default() += 1;
            let stat = read(path.join("stat")).unwrap_or_default();
            let state = stat
                .rsplit_once(") ")
                .and_then(|(_, tail)| tail.chars().next());
            if state == Some('D') {
                blocked.push(json!({
                    "tid": task.file_name().to_string_lossy(),
                    "stat": stat,
                    "syscall": read(path.join("syscall")),
                    "kernel_stack": read(path.join("stack"))
                }));
            }
        }
    }
    json!({
        "pid": pid,
        "stat": read(root.join("stat")),
        "io": read(root.join("io")),
        "wait_channels": waits,
        "uninterruptible_threads": blocked
    })
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let owner = format!("/proc/{}/stat", args.owner);
    let original = read(&owner).ok_or("qualification owner is not running")?;
    let identity = |stat: &str| {
        stat.rsplit_once(") ")
            .and_then(|(_, fields)| fields.split_whitespace().nth(19))
            .map(str::to_owned)
    };
    let owner_started = identity(&original).ok_or("invalid owner start time")?;
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(args.output)?;
    while read(&owner).as_deref().and_then(identity).as_ref() == Some(&owner_started) {
        let ready = std::fs::read(args.root.join("stack-ready.json"))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
        let processes = ready
            .as_ref()
            .into_iter()
            .flat_map(|ready| {
                ["object_pid", "metadata_pid"]
                    .into_iter()
                    .filter_map(move |role| ready[role].as_u64().map(|pid| (role, pid)))
            })
            .map(|(role, pid)| json!({"role":role,"process":process(pid)}))
            .collect::<Vec<_>>();
        let disks = read("/proc/diskstats")
            .unwrap_or_default()
            .lines()
            .filter(|line| {
                line.split_whitespace()
                    .nth(2)
                    .is_some_and(|name| matches!(name, "sdc" | "sdd"))
            })
            .map(str::to_owned)
            .collect::<Vec<_>>();
        writeln!(
            output,
            "{}",
            json!({
                "unix": m1::middleware::auth::now(),
                "processes": processes,
                "diskstats": disks,
                "io_pressure": read("/proc/pressure/io"),
                "meminfo": read("/proc/meminfo")
            })
        )?;
        output.flush()?;
        std::thread::sleep(std::time::Duration::from_secs(5));
    }
    Ok(())
}
