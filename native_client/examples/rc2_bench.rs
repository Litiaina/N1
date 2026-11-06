//! Bounded native benchmark client. Payloads are generated in memory and GETs
//! stream into a SHA-256 sink. Reports and client-held keys stay outside N1.
use n1_link::{ObjectKey, PoolLayout};
use n1_native_client::{Command, N1NativeClient, Response};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
#[path = "rc2_bench/checksums.rs"]
mod checksums;
#[path = "rc2_bench/comparison.rs"]
mod comparison;
#[path = "rc2_bench/scale.rs"]
mod scale;
#[path = "rc2_bench/supervisor.rs"]
mod supervisor;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const LARGE: u64 = 1024 * 1024 * 1024;
const SMALL: u64 = 4096;
const COUNT: usize = 100;

#[derive(Clone)]
struct Payload {
    block: Arc<Vec<u8>>,
    remaining: u64,
    offset: usize,
}
impl Payload {
    fn new(block: Arc<Vec<u8>>, bytes: u64) -> Self {
        Self {
            block,
            remaining: bytes,
            offset: 0,
        }
    }
}
impl AsyncRead for Payload {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let take = out
            .remaining()
            .min(self.remaining as usize)
            .min(self.block.len() - self.offset);
        out.put_slice(&self.block[self.offset..self.offset + take]);
        self.remaining -= take as u64;
        self.offset = (self.offset + take) % self.block.len();
        Poll::Ready(Ok(()))
    }
}
#[derive(Default)]
struct HashSink {
    hash: Sha256,
    bytes: u64,
}
impl AsyncWrite for HashSink {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.hash.update(bytes);
        self.bytes += bytes.len() as u64;
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
fn pattern(seed: u64) -> Arc<Vec<u8>> {
    let mut out = Vec::with_capacity(1024 * 1024);
    let mut state = seed;
    while out.len() < 1024 * 1024 {
        state = state.wrapping_add(0x9e3779b97f4a7c15);
        let mut value = state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
        value ^= value >> 31;
        out.extend_from_slice(&value.to_le_bytes());
    }
    Arc::new(out)
}
fn expected(block: &[u8], mut bytes: u64) -> String {
    let mut hash = Sha256::new();
    while bytes > 0 {
        let take = bytes.min(block.len() as u64) as usize;
        hash.update(&block[..take]);
        bytes -= take as u64;
    }
    format!("{:x}", hash.finalize())
}
fn field(value: &Value, name: &str) -> Result<String> {
    Ok(value[name]
        .as_str()
        .ok_or_else(|| format!("missing {name}"))?
        .into())
}
fn value(response: Response) -> Result<Value> {
    Ok(response.result.ok_or("missing result")?)
}
fn append(path: &Path, row: &Value) -> Result<()> {
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    serde_json::to_writer(&mut file, row)?;
    file.write_all(b"\n")?;
    file.sync_data()?;
    Ok(())
}
fn process(pid: u32) -> Result<Value> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status"))?;
    let kb = |name: &str| {
        status
            .lines()
            .find_map(|line| {
                line.strip_prefix(name)
                    .and_then(|v| v.split_whitespace().next()?.parse::<u64>().ok())
            })
            .map(|n| n * 1024)
    };
    let io = std::fs::read_to_string(format!("/proc/{pid}/io"))?;
    let count = |name: &str| {
        io.lines().find_map(|line| {
            line.strip_prefix(name)
                .and_then(|v| v.trim().parse::<u64>().ok())
        })
    };
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let fields = stat[stat.rfind(')').ok_or("invalid process stat")? + 1..]
        .split_whitespace()
        .collect::<Vec<_>>();
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
    let cpu = (fields[11].parse::<u64>()? + fields[12].parse::<u64>()?) as f64 / ticks;
    Ok(json!({
        "rss_bytes": kb("VmRSS:"),
        "vm_hwm_bytes": kb("VmHWM:"),
        "swap_bytes": kb("VmSwap:"),
        "read_bytes": count("read_bytes:"),
        "write_bytes": count("write_bytes:"),
        "cpu_seconds": cpu
    }))
}
struct Phase {
    start: Instant,
    before: Value,
    stop: Arc<AtomicBool>,
    sample: tokio::task::JoinHandle<Result<u64>>,
}
impl Phase {
    fn start(pid: u32) -> Result<Self> {
        let before = process(pid)?;
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let sample = tokio::spawn(async move {
            let mut peak = 0;
            while !stopping.load(Ordering::Relaxed) {
                peak = peak.max(process(pid)?["rss_bytes"].as_u64().unwrap_or(0));
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok(peak)
        });
        Ok(Self {
            start: Instant::now(),
            before,
            stop,
            sample,
        })
    }
    async fn finish(self, pid: u32, name: &str, bytes: u64, times: Vec<f64>) -> Result<Value> {
        let elapsed = self.start.elapsed().as_secs_f64();
        let after = process(pid)?;
        self.stop.store(true, Ordering::Relaxed);
        let peak = self
            .sample
            .await??
            .max(self.before["rss_bytes"].as_u64().unwrap_or(0))
            .max(after["rss_bytes"].as_u64().unwrap_or(0));
        let mut sorted = times.clone();
        sorted.sort_by(f64::total_cmp);
        let percentile = |p: f64| {
            sorted
                .get(((sorted.len() as f64 * p).ceil() as usize).saturating_sub(1))
                .copied()
        };
        let delta = |field: &str| {
            after[field]
                .as_u64()
                .unwrap_or(0)
                .saturating_sub(self.before[field].as_u64().unwrap_or(0))
        };
        Ok(json!({
            "phase": name,
            "wall_seconds": elapsed,
            "source_bytes": bytes,
            "operations": times.len(),
            "mib_per_second": bytes as f64/1048576.0/elapsed,
            "operations_per_second": times.len()as f64/elapsed,
            "latency_ms": {
                "min": sorted.first().map(|v|v*1000.0),
                "p50": percentile(0.50).map(|v|v*1000.0),
                "p95": percentile(0.95).map(|v|v*1000.0),
                "p99": percentile(0.99).map(|v|v*1000.0),
                "max": sorted.last().map(|v|v*1000.0),
                "mean": if times.is_empty(){
                    None
                }else{
                    Some(times.iter().sum::<f64>()/times.len()as f64*1000.0)
                }
            },
            "samples_seconds": times,
            "rss_before_bytes": self.before["rss_bytes"],
            "rss_after_bytes": after["rss_bytes"],
            "sampled_peak_rss_bytes": peak,
            "server_cpu_seconds": after["cpu_seconds"].as_f64().unwrap_or(0.0)-self.before["cpu_seconds"].as_f64().unwrap_or(0.0),
            "server_read_bytes": delta("read_bytes"),
            "server_write_bytes": delta("write_bytes"),
            "after": after
        }))
    }
}
#[derive(Clone)]
struct Object {
    key: String,
    ticket: String,
    bytes: u64,
    hash: String,
}
async fn init(
    client: &mut N1NativeClient,
    bytes: u64,
    stripe: u64,
    hash: String,
) -> Result<Object> {
    let response = value(
        client
            .control(Command::UploadInit {
                pool: None,
                total_bytes: bytes,
                part_bytes: bytes.max(1).div_ceil(stripe) * stripe,
                durable: true,
                ttl_seconds: 86400,
            })
            .await?,
    )?;
    Ok(Object {
        key: field(&response, "object_key")?,
        ticket: field(&response, "object_key")?,
        bytes,
        hash,
    })
}
async fn get(client: &mut N1NativeClient, object: &Object) -> Result<()> {
    let mut sink = HashSink::default();
    let (_, read) = client
        .download(object.key.clone(), 0, None, &mut sink)
        .await?;
    if read != object.bytes
        || sink.bytes != object.bytes
        || format!("{:x}", sink.hash.finalize()) != object.hash
    {
        return Err("download length/SHA-256 mismatch".into());
    }
    Ok(())
}
fn footprint(
    objects: &[Object],
    layout: &PoolLayout,
    roots: &[PathBuf],
    evict: bool,
) -> Result<Value> {
    let mut files = 0;
    let mut logical = 0;
    let mut allocated = 0;
    for object in objects {
        let fp = ObjectKey::inspect(&object.key)?.fingerprint();
        for shard in 0..layout.width() {
            let path =
                roots[layout.member_slot(fp, shard as u16)?].join(fp.shard_path(shard as u16));
            let file = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW)
                .open(path)?;
            let meta = file.metadata()?;
            if !meta.is_file() {
                return Err("nonregular calculated benchmark shard".into());
            }
            files += 1;
            logical += meta.len();
            allocated += meta.blocks() * 512;
            if evict {
                let error = unsafe {
                    libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED)
                };
                if error != 0 {
                    return Err(std::io::Error::from_raw_os_error(error).into());
                }
            }
        }
    }
    let source = objects.iter().map(|o| o.bytes).sum::<u64>();
    Ok(json!({
        "files": files,
        "source_bytes": source,
        "logical_shard_bytes": logical,
        "allocated_shard_bytes": allocated,
        "logical_amplification": logical as f64/source as f64,
        "allocated_amplification": allocated as f64/source as f64,
        "payload_cache_drop_requested": evict,
        "includes_directory_metadata": false
    }))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("benchmark failed: {error}");
        std::process::exit(1);
    }
}
async fn run() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.get(1).is_some_and(|arg| {
        matches!(
            arg.as_str(),
            "--scale" | "--scale-smoke" | "--scale-resume" | "--scale-test-resume"
        )
    }) {
        return scale::run(args).await;
    }
    if args.get(1).is_some_and(|arg| arg == "--profile-checksums") {
        return checksums::run(args);
    }
    if args.get(1).is_some_and(|arg| {
        matches!(
            arg.as_str(),
            "--compare"
                | "--compare-smoke"
                | "--compare-link"
                | "--compare-reads-link"
                | "--compare-reads-direct"
                | "--compare-integrity"
                | "--compare-key-purge"
                | "--compare-key-performance"
                | "--compare-upload-paths"
                | "--compare-upload-paths-smoke"
                | "--compare-large-link"
                | "--compare-large-ncx"
                | "--compare-aligned-io"
                | "--compare-aligned-large"
                | "--compare-aligned-uploads"
                | "--compare-aligned-smoke"
                | "--compare-reads-ncx"
        )
    }) {
        return comparison::run(args).await;
    }
    if args
        .get(1)
        .is_some_and(|arg| arg == "--run" || arg == "--run-small")
    {
        return supervisor::run(args).await;
    }
    run_benchmark(args, true).await
}
async fn run_benchmark(args: Vec<String>, include_large: bool) -> Result<()> {
    if args.len() != 5 {
        return Err(
            "usage: rc2_bench credentials.json service.json report-directory server-pid".into(),
        );
    }
    let report = Path::new(&args[3]).canonicalize()?;
    if !report.starts_with("/mnt/PROJECT-DISK-1TB") {
        return Err("benchmark reports must be on PROJECT HDD".into());
    }
    let events = report.join("measurements.jsonl");
    let manifest = report.join("client-manifest.jsonl");
    if events.exists() || manifest.exists() {
        return Err("refusing to overwrite benchmark evidence".into());
    }
    let pid = args[4].parse::<u32>()?;
    let credentials: Value = serde_json::from_reader(File::open(&args[1])?)?;
    let config: Value = serde_json::from_reader(File::open(&args[2])?)?;
    let layout: PoolLayout = serde_json::from_value(config["pools"][0]["layout"].clone())?;
    let roots: Vec<PathBuf> = serde_json::from_value(config["pools"][0]["roots"].clone())?;
    if roots.iter().any(|p| !isolated_fixture_root(p)) {
        return Err("member roots are not isolated benchmark fixtures".into());
    }
    let mut client = N1NativeClient::connect(&field(&credentials, "native_url")?).await?;
    client
        .auth_fragment(
            &field(&credentials, "fragment")?,
            &field(&credentials, "secret")?,
        )
        .await?;
    let start_health = value(client.control(Command::Health).await?)?;
    let block = pattern(0x20261102);
    let hash_started = Instant::now();
    let large_hash = if include_large {
        expected(&block, LARGE)
    } else {
        String::new()
    };
    let small_hash = expected(&block, SMALL);
    append(
        &events,
        &json!({
            "phase": "methodology",
            "build": if cfg!(debug_assertions){
                "debug"
            }else{
                "release"
            },
            "large_bytes": if include_large {
                LARGE
            } else {
                0
            },
            "small_bytes": SMALL,
            "small_count_per_method": COUNT,
            "concurrency": 1,
            "source": "repeating 1 MiB SplitMix64 pattern; bounded streaming generator",
            "download": "SHA-256 sink; verification included in GET wall time",
            "initialization_and_auth": "auth excluded; init timed separately",
            "durability": "normal/batch acknowledgement includes final persistence barriers",
            "cache": "per-shard fadvise DONTNEED before cold reads; warm reads follow; no global cache drop",
            "expected_hash_compute_seconds": hash_started.elapsed().as_secs_f64(),
            "large_sha256": large_hash,
            "small_sha256": small_hash,
            "before": start_health
        }),
    )?;
    let stripe = u64::from(layout.data_shards) * u64::from(layout.block_bytes);
    let mut all = Vec::new();
    if include_large {
        // Large normal PUT: one bounded streaming source, no 1 GiB client buffer.
        let phase = Phase::start(pid)?;
        let timer = Instant::now();
        let large = init(&mut client, LARGE, stripe, large_hash).await?;
        append(
            &events,
            &phase
                .finish(pid, "large_init", 0, vec![timer.elapsed().as_secs_f64()])
                .await?,
        )?;
        append(
            &manifest,
            &json!({"cohort":"large","key":large.key,"ticket":large.ticket,"bytes":large.bytes,"sha256":large.hash}),
        )?;
        let phase = Phase::start(pid)?;
        let timer = Instant::now();
        client
            .put(
                large.ticket.clone(),
                LARGE,
                &mut Payload::new(block.clone(), LARGE),
            )
            .await?;
        append(
            &events,
            &phase
                .finish(pid, "large_put", LARGE, vec![timer.elapsed().as_secs_f64()])
                .await?,
        )?;
        append(
            &events,
            &json!({"phase":"large_footprint","storage":footprint(std::slice::from_ref(&large),&layout,&roots,true)?}),
        )?;
        for name in ["large_get_cold_requested", "large_get_warm"] {
            let phase = Phase::start(pid)?;
            let timer = Instant::now();
            get(&mut client, &large).await?;
            append(
                &events,
                &phase
                    .finish(pid, name, LARGE, vec![timer.elapsed().as_secs_f64()])
                    .await?,
            )?;
        }
        let phase = Phase::start(pid)?;
        let timer = Instant::now();
        let health = value(
            client
                .control(Command::ObjectHealth {
                    object_key: large.key.clone(),
                    deep: true,
                })
                .await?,
        )?;
        if health["status"] != "healthy" {
            return Err("large object deep health failed".into());
        }
        append(
            &events,
            &phase
                .finish(
                    pid,
                    "large_deep_health",
                    LARGE,
                    vec![timer.elapsed().as_secs_f64()],
                )
                .await?,
        )?;
        all.push(large);
    }
    // Independent cohorts; batch never reuses the normal cohort's final files.
    for method in ["individual", "batch"] {
        let mut objects = Vec::with_capacity(COUNT);
        let mut times = Vec::with_capacity(COUNT);
        let phase = Phase::start(pid)?;
        for _ in 0..COUNT {
            let timer = Instant::now();
            objects.push(init(&mut client, SMALL, stripe, small_hash.clone()).await?);
            times.push(timer.elapsed().as_secs_f64());
        }
        append(
            &events,
            &phase
                .finish(pid, &format!("small_{method}_init"), 0, times)
                .await?,
        )?;
        for object in &objects {
            append(
                &manifest,
                &json!({"cohort":method,"key":object.key,"ticket":object.ticket,"bytes":object.bytes,"sha256":object.hash}),
            )?;
        }
        let phase = Phase::start(pid)?;
        let mut times = Vec::new();
        if method == "individual" {
            for object in &objects {
                let timer = Instant::now();
                client
                    .put(
                        object.ticket.clone(),
                        SMALL,
                        &mut Payload::new(block.clone(), SMALL),
                    )
                    .await?;
                times.push(timer.elapsed().as_secs_f64());
            }
        } else {
            let tickets = objects.iter().map(|o| o.ticket.clone()).collect::<Vec<_>>();
            let body = block[..SMALL as usize].repeat(COUNT);
            let timer = Instant::now();
            client
                .put_batch(tickets, body.len() as u64, &mut std::io::Cursor::new(body))
                .await?;
            times.push(timer.elapsed().as_secs_f64());
        }
        let mut row = phase
            .finish(
                pid,
                &format!("small_{method}_put"),
                SMALL * COUNT as u64,
                times,
            )
            .await?;
        row["object_count"] = json!(COUNT);
        row["objects_per_second"] =
            json!(COUNT as f64 / row["wall_seconds"].as_f64().ok_or("missing duration")?);
        append(&events, &row)?;
        append(
            &events,
            &json!({"phase":format!("small_{method}_footprint"),"storage":footprint(&objects,&layout,&roots,true)?}),
        )?;
        for temperature in ["cold_requested", "warm"] {
            let phase = Phase::start(pid)?;
            let mut times = Vec::new();
            for object in &objects {
                let timer = Instant::now();
                get(&mut client, object).await?;
                times.push(timer.elapsed().as_secs_f64());
            }
            append(
                &events,
                &phase
                    .finish(
                        pid,
                        &format!("small_{method}_get_{temperature}"),
                        SMALL * COUNT as u64,
                        times,
                    )
                    .await?,
            )?;
        }
        let phase = Phase::start(pid)?;
        let mut times = Vec::new();
        for object in &objects {
            let timer = Instant::now();
            client
                .control(Command::ObjectStat {
                    object_key: object.key.clone(),
                })
                .await?;
            times.push(timer.elapsed().as_secs_f64());
        }
        append(
            &events,
            &phase
                .finish(pid, &format!("small_{method}_stat"), 0, times)
                .await?,
        )?;
        all.extend(objects);
    }
    let phase = Phase::start(pid)?;
    let mut times = Vec::new();
    for object in &all {
        let timer = Instant::now();
        client
            .control(Command::ObjectDelete {
                object_key: object.key.clone(),
            })
            .await?;
        times.push(timer.elapsed().as_secs_f64());
    }
    append(
        &events,
        &phase
            .finish(pid, &format!("delete_all_{}", all.len()), 0, times)
            .await?,
    )?;
    for object in &all {
        match client
            .control(Command::ObjectStat {
                object_key: object.key.clone(),
            })
            .await
        {
            Err(n1_native_client::NativeClientError::Native { code, .. }) if code == "404" => {}
            _ => return Err("deleted key did not remain retired".into()),
        }
    }
    append(
        &events,
        &json!({
            "phase": "complete",
            "result": "passed",
            "objects": all.len(),
            "download_sha256_checks": 2*all.len(),
            "after": value(client.control(Command::Health).await?)?
        }),
    )?;
    println!("benchmark passed; measurements: {}", events.display());
    Ok(())
}

fn isolated_fixture_root(path: &Path) -> bool {
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return false;
    }
    let Some(fixture) = path.parent() else {
        return false;
    };
    fixture
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with("n1-benchmark-"))
        && fixture.parent().is_some_and(|parent| {
            parent == Path::new("/mnt/PROJECT-DISK-1TB")
                || parent == Path::new("/media/altear/TEST-1TB/mx-tests")
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    #[test]
    fn fixture_guard_accepts_benchmark_children_and_rejects_unrelated_or_traversed_roots() {
        for path in [
            "/mnt/PROJECT-DISK-1TB/n1-benchmark-restored-123/member-0",
            "/media/altear/TEST-1TB/mx-tests/n1-benchmark-20261102-01/member-1",
        ] {
            assert!(isolated_fixture_root(Path::new(path)), "{path}");
        }
        for path in [
            "/home/altear/Development/N1/member-0",
            "/mnt/PROJECT-DISK-1TB/production/member-0",
            "/mnt/PROJECT-DISK-1TB/n1-benchmark-other/../production/member-0",
            "/media/altear/TEST-1TB/n1-benchmark-other/member-0",
        ] {
            assert!(!isolated_fixture_root(Path::new(path)), "{path}");
        }
    }
    #[tokio::test]
    async fn bounded_source_and_hash_sink_preserve_exact_bytes_across_read_boundaries() {
        let block = Arc::new(vec![1, 19, 255, 0, 6, 7, 8]);
        let length = 10003;
        let mut source = Payload::new(block.clone(), length);
        let mut received = Vec::new();
        let mut buf = [0; 19];
        loop {
            let n = source.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            received.extend_from_slice(&buf[..n]);
        }
        let wanted = block
            .iter()
            .copied()
            .cycle()
            .take(length as usize)
            .collect::<Vec<_>>();
        assert_eq!(received, wanted);
        assert_eq!(
            expected(&block, length),
            format!("{:x}", Sha256::digest(&wanted))
        );
        let mut sink = HashSink::default();
        tokio::io::copy(&mut std::io::Cursor::new(received), &mut sink)
            .await
            .unwrap();
        assert_eq!(sink.bytes, length);
        assert_eq!(
            format!("{:x}", sink.hash.finalize()),
            expected(&block, length)
        );
    }
}
