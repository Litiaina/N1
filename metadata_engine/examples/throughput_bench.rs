//! Bounded, server-local named throughput measurement against an isolated M1.
//! Never provisions fragments in the interactive Console's metadata database.
#[path = "throughput_bench/measure.rs"]
mod measure;
#[path = "throughput_bench/named.rs"]
mod named;
#[path = "throughput_bench/standalone.rs"]
mod standalone;

use bytes::Bytes;
use clap::Parser;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{fs, io::Write, os::unix::fs::PermissionsExt, path::PathBuf, time::Duration};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const LARGE: u64 = 1 << 30;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    output: PathBuf,
    /// Measure only 100-object batches; do not repeat the large-file campaign.
    #[arg(long)]
    batch_only: bool,
    /// Measure N1 directly over native TLS, without sending requests to M1.
    #[arg(long, conflicts_with = "batch_only")]
    standalone_n1: bool,
}
#[derive(Deserialize)]
struct Config {
    url: String,
    ca: PathBuf,
    master_env: PathBuf,
    object_pid: u32,
    metadata_pid: u32,
    console_pid: u32,
    #[serde(default = "repeats")]
    repeats: usize,
}
fn repeats() -> usize {
    3
}
fn write_json(path: &std::path::Path, value: &Value) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}
fn block() -> Bytes {
    let mut bytes = Vec::with_capacity(1 << 20);
    let mut state = 0x20261112_u64;
    while bytes.len() < 1 << 20 {
        state = state.wrapping_add(0x9e3779b97f4a7c15);
        let mut word = state;
        word = (word ^ (word >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        word = (word ^ (word >> 27)).wrapping_mul(0x94d049bb133111eb);
        bytes.extend_from_slice(&(word ^ (word >> 31)).to_le_bytes());
    }
    bytes.into()
}
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args = Args::parse();
    if let Err(error) = run(&args).await {
        let _ = write_json(
            &args.output.join("FAILED.json"),
            &json!({"error":error.to_string()}),
        );
        eprintln!("throughput measurement failed: {error}");
        std::process::exit(1);
    }
}
async fn run(args: &Args) -> Result<()> {
    if cfg!(debug_assertions) {
        return Err("use an optimized release-profile benchmark".into());
    }
    for path in [&args.config, &args.output] {
        if !path.is_absolute()
            || fs::symlink_metadata(path)?.file_type().is_symlink()
            || fs::metadata(path)?.permissions().mode() & 0o077 != 0
        {
            return Err("benchmark paths must be private, absolute and real".into());
        }
    }
    if fs::read_dir(&args.output)?.next().is_some() {
        return Err("benchmark output must be empty".into());
    }
    if args.standalone_n1 {
        return standalone::run(&args.config, &args.output).await;
    }
    let config: Config = serde_json::from_slice(&fs::read(&args.config)?)?;
    if !(1..=5).contains(&config.repeats) {
        return Err("repeats must be 1..=5".into());
    }
    if config.object_pid == 0
        || config.metadata_pid == 0
        || config.object_pid == config.metadata_pid
    {
        return Err("named measurements require distinct running N1 and M1 process IDs".into());
    }
    let url = reqwest::Url::parse(&config.url)?;
    if !matches!(url.host_str(), Some("127.0.0.1" | "localhost")) || url.scheme() != "https" {
        return Err("benchmark must target the isolated loopback TLS M1".into());
    }
    let ca = reqwest::Certificate::from_pem(&fs::read(&config.ca)?)?;
    let client = reqwest::Client::builder()
        .add_root_certificate(ca)
        .timeout(Duration::from_secs(900))
        .tcp_nodelay(true)
        .build()?;
    let master = fs::read_to_string(&config.master_env)?
        .lines()
        .find_map(|line| line.strip_prefix("MASTER_KEY=").map(str::to_owned))
        .filter(|key| !key.is_empty())
        .ok_or("missing isolated M1 master key")?;
    let pids = vec![
        ("N1", config.object_pid),
        ("M1", config.metadata_pid),
        ("interactive_Console", config.console_pid),
        ("client", std::process::id()),
    ]
    .into_iter()
    .filter(|(_, pid)| *pid != 0)
    .collect::<Vec<_>>();
    let source = block();
    let mut hash = Sha256::new();
    for _ in 0..1024 {
        hash.update(&source);
    }
    let expected = hex::encode(hash.finalize());
    let run = ulid::Ulid::new().to_string();
    let api = named::Api::create(client, &config.url, &master, &run).await?;
    write_json(
        &args.output.join("methodology.json"),
        &json!({
        "run":run,"repeats":config.repeats,"small_count":100,"small_bytes":4096,"batch_only":args.batch_only,
            "large_bytes":LARGE,"large_sha256":expected,"strict_durability":true,
            "payload":"repeated 1 MiB SplitMix64 block; prepared before timing; bounded buffers",
            "download":"streamed SHA-256 and exact length/version verification included in timing",
            "cache_policy":"no global cache drop; immediate post-upload GET; no cold-read claim",
            "path":"server-local HTTPS -> isolated NVMe M1 -> existing native TLS N1 -> HDD 3+1",
            "interactive_database_modified":false,"pids":pids,
            "client_sha256":hex::encode(Sha256::digest(fs::read(std::env::current_exe()?)?)),
            "cpu_percent":"100% means one logical CPU; process CPU delta divided by phase wall time",
            "rss":"50 ms observed samples; process lifetime HWM is recorded separately"
        }),
    )?;
    let mut rows = Vec::new();
    for repeat in 0..config.repeats {
        if args.batch_only {
            let mut row = named::batch(&api, &run, repeat, source.slice(..4096), &pids).await?;
            row["repeat"] = json!(repeat + 1);
            write_json(
                &args.output.join(format!("batch-r{}.json", repeat + 1)),
                &row,
            )?;
            println!(
                "{}",
                json!({"completed":row["method"],"repeat":repeat+1,"upload_seconds":row["upload"]["wall_seconds"]})
            );
            rows.push(row);
            continue;
        }
        for concurrency in [1, 8] {
            let mut row =
                named::small(&api, &run, repeat, concurrency, source.slice(..4096), &pids).await?;
            row["repeat"] = json!(repeat + 1);
            write_json(
                &args
                    .output
                    .join(format!("small-r{}-c{concurrency}.json", repeat + 1)),
                &row,
            )?;
            println!(
                "{}",
                json!({"completed":row["method"],"repeat":repeat+1,"upload_seconds":row["upload"]["wall_seconds"]})
            );
            rows.push(row);
        }
        for part_mib in [0, 8, 48] {
            let mut row = named::large(
                &api,
                &run,
                repeat,
                part_mib,
                source.clone(),
                &expected,
                &pids,
            )
            .await?;
            row["repeat"] = json!(repeat + 1);
            write_json(
                &args
                    .output
                    .join(format!("large-r{}-part{part_mib}.json", repeat + 1)),
                &row,
            )?;
            println!(
                "{}",
                json!({"completed":row["method"],"repeat":repeat+1,"upload_seconds":row["upload"]["wall_seconds"],"mib_per_second":row["upload"]["mib_per_second"]})
            );
            rows.push(row);
        }
    }
    write_json(
        &args.output.join("MEASUREMENTS.json"),
        &json!({"rows":rows,"all_downloads_verified":true}),
    )?;
    let cleanup = named::cleanup(&api, &run, config.repeats, args.batch_only).await?;
    write_json(
        &args.output.join("PASSED.json"),
        &json!({"passed":true,"rows":rows,"all_downloads_verified":true,"cleanup":cleanup}),
    )
}
