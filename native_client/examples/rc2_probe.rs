//! Local headless qualification witness. Keys live only in this external client
//! manifest, never an engine catalogue. Run write, restart the server, then verify.
use n1_native_client::{Command, N1NativeClient, NativeClientError};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    os::unix::fs::OpenOptionsExt,
    path::Path,
    time::Instant,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
fn text(v: &Value, name: &str) -> Result<String> {
    Ok(v[name].as_str().ok_or("missing string")?.into())
}
fn data(index: usize, size: usize) -> Vec<u8> {
    (0..size)
        .map(|offset| ((offset.wrapping_mul(197) + index * 31) % 251) as u8)
        .collect()
}
fn append(path: &Path, value: Value) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .custom_flags(libc_flags())
        .open(path)?;
    serde_json::to_writer(&mut file, &value)?;
    file.write_all(b"\n")?;
    file.sync_data()?;
    Ok(())
}
// O_NOFOLLOW on Linux; the probe SDK also builds on other client platforms.
fn libc_flags() -> i32 {
    #[cfg(target_os = "linux")]
    {
        libc::O_NOFOLLOW
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}
fn rss() -> Option<u64> {
    let pid = std::env::var("N1_SERVER_PID").ok()?;
    let content = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    content
        .lines()
        .find(|s| s.starts_with("VmRSS:"))?
        .split_whitespace()
        .nth(1)?
        .parse::<u64>()
        .ok()
        .map(|n| n * 1024)
}
#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("RC.2 witness failed: {e}");
        std::process::exit(1);
    }
}
async fn run() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 5 {
        return Err("usage: rc2_probe write|verify|retire <credentials.json> <manifest.jsonl> <report.jsonl> [count]".into());
    }
    let mode = &args[1];
    let credentials: Value = serde_json::from_reader(File::open(&args[2])?)?;
    let manifest = Path::new(&args[3]);
    let report = Path::new(&args[4]);
    for path in [manifest, report] {
        let parent = path
            .parent()
            .ok_or("missing output parent")?
            .canonicalize()?;
        if !["/mnt/test-project", "/mnt/test-secondary", "/tmp"]
            .iter()
            .any(|root| parent.starts_with(root))
        {
            return Err("witness output must be on an HDD or RAM".into());
        }
    }
    let mut client = N1NativeClient::connect(&text(&credentials, "native_url")?).await?;
    client
        .auth_fragment(
            &text(&credentials, "fragment")?,
            &text(&credentials, "secret")?,
        )
        .await?;
    let before = client.control(Command::Health).await?.result;
    let mut times = Vec::new();
    let mut count = 0;
    let mut bytes = 0;
    let start = Instant::now();
    if mode == "write" {
        if manifest.exists() {
            return Err("refusing to replace an existing witness manifest".into());
        }
        let n = args
            .get(5)
            .map(|n| n.parse())
            .transpose()?
            .unwrap_or(64usize);
        if n == 0 || n > 1_000_000 {
            return Err("count must be 1..=1000000".into());
        }
        let pools = client
            .control(Command::Pools)
            .await?
            .result
            .ok_or("missing pools")?;
        let pool = pools["pools"]
            .as_array()
            .and_then(|p| p.last())
            .ok_or("no pool")?;
        let stripe = pool["data_shards"].as_u64().ok_or("missing K")?
            * pool["block_bytes"].as_u64().ok_or("missing block")?;
        for batch_start in (0..n).step_by(8) {
            let mut records = Vec::new();
            let mut tickets = Vec::new();
            let mut body = Vec::new();
            for index in batch_start..(batch_start + 8).min(n) {
                let size = [0, 1, 4095, 65536, 1024 * 1024, 4 * 1024 * 1024][index % 6];
                let source = data(index, size);
                let hash = hex_hash(&source);
                let init = client
                    .control(Command::UploadInit {
                        pool: None,
                        total_bytes: size as u64,
                        part_bytes: (size as u64).max(1).div_ceil(stripe) * stripe,
                        durable: true,
                        ttl_seconds: 86400,
                    })
                    .await?
                    .result
                    .ok_or("missing upload")?;
                tickets.push(text(&init, "object_key")?);
                body.extend_from_slice(&source);
                records.push(json!({"index":index,"size":size,"sha256":hash,"object_key":text(&init,"object_key")?}));
                bytes += size;
            }
            let timer = Instant::now();
            let response = if batch_start % 16 == 0 {
                client
                    .put_batch(tickets, body.len() as u64, &mut std::io::Cursor::new(body))
                    .await?
            } else {
                let mut offset = 0;
                for record in &records {
                    let length = record["size"].as_u64().ok_or("missing size")? as usize;
                    client
                        .put(
                            text(record, "object_key")?,
                            length as u64,
                            &mut std::io::Cursor::new(&body[offset..offset + length]),
                        )
                        .await?;
                    offset += length;
                }
                client.control(Command::Ping).await?
            };
            let elapsed = timer.elapsed().as_secs_f64();
            times.push(elapsed);
            let _ = response;
            append(
                report,
                json!({
                    "operation": if batch_start%16==0{
                        "batch"
                    }else{
                        "normal_group"
                    },
                    "count": records.len(),
                    "seconds": elapsed,
                    "rss_bytes": rss()
                }),
            )?;
            for record in records {
                append(manifest, record)?;
                count += 1;
            }
        }
    } else {
        for line in BufReader::new(File::open(manifest)?).lines() {
            let record: Value = serde_json::from_str(&line?)?;
            let key = text(&record, "object_key")?;
            let ticket = text(&record, "object_key")?;
            let timer = Instant::now();
            if mode == "verify" {
                let mut received = Vec::new();
                client.download(key.clone(), 0, None, &mut received).await?;
                let index = record["index"].as_u64().ok_or("missing index")? as usize;
                let size = record["size"].as_u64().ok_or("missing size")? as usize;
                if received != data(index, size) || hex_hash(&received) != record["sha256"] {
                    return Err(format!("content mismatch for witness item {index}").into());
                }
                client
                    .control(Command::UploadFinalize { object_key: ticket })
                    .await?;
                client
                    .control(Command::ObjectHealth {
                        object_key: key,
                        deep: true,
                    })
                    .await?;
                bytes += size;
            } else if mode == "retire" {
                client
                    .control(Command::ObjectDelete {
                        object_key: key.clone(),
                    })
                    .await?;
                for command in [
                    Command::ObjectStat { object_key: key },
                    Command::UploadFinalize { object_key: ticket },
                ] {
                    match client.control(command).await {
                        Err(NativeClientError::Native { code, .. }) if code == "410" => {}
                        other => {
                            return Err(format!("retirement did not return 410: {other:?}").into());
                        }
                    }
                }
            } else {
                return Err("unknown witness mode".into());
            }
            let seconds = timer.elapsed().as_secs_f64();
            times.push(seconds);
            count += 1;
            append(
                report,
                json!({"operation":mode,"index":record["index"],"seconds":seconds,"rss_bytes":rss()}),
            )?;
        }
    }
    times.sort_by(f64::total_cmp);
    let percentile = |p: f64| {
        times
            .get(((times.len().saturating_sub(1)) as f64 * p) as usize)
            .copied()
    };
    let after = client.control(Command::Health).await?.result;
    let summary = json!({
        "result": "passed",
        "scope": "headless local witness; no scale or power-loss qualification",
        "mode": mode,
        "objects": count,
        "source_bytes": bytes,
        "wall_seconds": start.elapsed().as_secs_f64(),
        "sample_p50_seconds": percentile(0.50),
        "sample_p95_seconds": percentile(0.95),
        "sample_p99_seconds": percentile(0.99),
        "build": if cfg!(debug_assertions){
            "debug"
        }else{
            "release"
        },
        "before": before,
        "after": after
    });
    append(report, summary.clone())?;
    println!("{}", serde_json::to_string(&summary)?);
    Ok(())
}
fn hex_hash(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
