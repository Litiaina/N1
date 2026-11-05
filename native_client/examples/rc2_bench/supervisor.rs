//! Rust process supervisor for a fresh, isolated, two-filesystem benchmark.
//! It never opens existing object roots or signals a process it did not spawn.
use super::{Result, run_benchmark};
use n1_native_client::{Command, N1NativeClient};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    process::{Child, Command as Process, ExitStatus, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub(super) struct Owned(pub(super) Child);
impl Drop for Owned {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
pub(super) fn private_json(path: &Path, value: &Value) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}
pub(super) async fn wait(child: &mut Child, seconds: u64) -> Result<ExitStatus> {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err("owned benchmark process exceeded deadline".into());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

pub async fn run(args: Vec<String>) -> Result<()> {
    if args.len() != 5 {
        return Err(
            "usage: rc2_bench --run /absolute/litiaina-n1 PROJECT-parent TEST-parent".into(),
        );
    }
    if cfg!(debug_assertions) {
        return Err("supervised benchmark requires a release-profile client".into());
    }
    let binary = PathBuf::from(&args[2]).canonicalize()?;
    let project = PathBuf::from(&args[3]).canonicalize()?;
    let test = PathBuf::from(&args[4]).canonicalize()?;
    if project != Path::new("/mnt/HDD1TB001")
        || test != Path::new("/mnt/HDD1TB002")
        || project.metadata()?.dev() == test.metadata()?.dev()
    {
        return Err(
            "benchmark requires the independent /mnt/HDD1TB001 and /mnt/HDD1TB002 mounts".into(),
        );
    }
    let name = format!(
        "n1-benchmark-restored-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let first = project.join(&name);
    let second = test.join(&name);
    for path in [&first, &second] {
        fs::DirBuilder::new().mode(0o700).create(path)?;
    }
    let result = execute(&binary, &first, &second, args[1] == "--run-small").await;
    if let Err(error) = &result {
        private_json(
            &first.join("failed.json"),
            &json!({"error":error.to_string()}),
        )?;
    }
    result
}

async fn execute(binary: &Path, first: &Path, second: &Path, small_only: bool) -> Result<()> {
    let roots = [first.join("member-0"), second.join("member-1")];
    for root in &roots {
        fs::create_dir(root)?;
    }
    let reports = first.join("reports");
    fs::DirBuilder::new().mode(0o700).create(&reports)?;
    let config = first.join("service.json");
    let credentials = config.with_extension("credentials.json");
    let address = std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?;
    let log = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(first.join("engine-init.log"))?;
    let mut init = Owned(
        Process::new(binary)
            .args(["init", "--config"])
            .arg(&config)
            .arg("--member")
            .arg(&roots[0])
            .arg("--member")
            .arg(&roots[1])
            .args([
                "--data-shards",
                "1",
                "--parity-shards",
                "1",
                "--native-address",
            ])
            .arg(address.to_string())
            .arg("--block-bytes")
            .arg(if small_only { "4096" } else { "262144" })
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .spawn()?,
    );
    if !wait(&mut init.0, 60).await?.success() {
        return Err("benchmark enrollment failed; see private engine-init.log".into());
    }
    drop(init);
    let mut configuration: Value = serde_json::from_reader(fs::File::open(&config)?)?;
    configuration["rate_limits"]["enabled"] = json!(false);
    fs::write(&config, serde_json::to_vec_pretty(&configuration)?)?;
    let executable = std::env::current_exe()?;
    private_json(
        &reports.join("provenance.json"),
        &json!({
            "engine_binary":binary,"engine_sha256":format!("{:x}",Sha256::digest(fs::read(binary)?)),
            "client_sha256":format!("{:x}",Sha256::digest(fs::read(executable)?)),"profile":"release",
            "native_endpoint":format!("n1://{address}"),"http_enabled":false,"io_backend":"required io_uring",
            "pool":configuration["pools"][0]["layout"],"concurrency":configuration["concurrency"],
            "max_operations":configuration["max_operations"],"operation_memory_bytes":configuration["operation_memory_bytes"],
            "rate_limits_enabled":false,"quota_note":"quotas excluded to measure storage; adaptive controller and resource limits enabled",
            "project_device":first.metadata()?.dev(),"test_device":second.metadata()?.dev(),
            "kernel":fs::read_to_string("/proc/sys/kernel/osrelease")?.trim(),
            "cpu_model":fs::read_to_string("/proc/cpuinfo")?.lines().find(|line|line.starts_with("model name")),
            "scope":if small_only {"shared local workstation; one serial client; 100 individual and 100 fresh batched 4 KiB objects; 4 KiB block pool"}
                else {"shared local workstation; one serial client; 1 GiB + 100 individual and 100 fresh batched 4 KiB objects"}
        }),
    )?;
    let log = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(first.join("engine.log"))?;
    let mut server = Owned(
        Process::new(binary)
            .args(["serve", "--config"])
            .arg(&config)
            .env("N1_IO_BACKEND", "uring")
            .env("RUST_LOG", "info")
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .spawn()?,
    );
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = server.0.try_wait()? {
            return Err(format!("benchmark server exited: {status}").into());
        }
        match N1NativeClient::connect(&format!("n1://{address}")).await {
            Ok(mut client) => {
                client.control(Command::Ping).await?;
                break;
            }
            Err(error) if Instant::now() >= deadline => return Err(error.into()),
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    let measurements = run_benchmark(
        vec![
            "rc2_bench".into(),
            credentials.display().to_string(),
            config.display().to_string(),
            reports.display().to_string(),
            server.0.id().to_string(),
        ],
        !small_only,
    )
    .await;
    // The owned server has not been reaped, so its PID cannot refer to another process.
    if unsafe { libc::kill(server.0.id() as i32, libc::SIGTERM) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let status = wait(&mut server.0, 40).await?;
    measurements?;
    if !status.success() {
        return Err(format!("benchmark server did not stop cleanly: {status}").into());
    }
    summarize(&reports)?;
    println!(
        "Optimized benchmark and clean shutdown passed: {}",
        reports.display()
    );
    Ok(())
}
fn summarize(reports: &Path) -> Result<()> {
    let mut rows = Vec::new();
    let mut footprint = Vec::new();
    let mut peak = 0u64;
    let mut hwm = 0u64;
    for line in BufReader::new(fs::File::open(reports.join("measurements.jsonl"))?).lines() {
        let value: Value = serde_json::from_str(&line?)?;
        if let Some(rss) = value["sampled_peak_rss_bytes"].as_u64() {
            peak = peak.max(rss);
        }
        hwm = hwm.max(value["after"]["vm_hwm_bytes"].as_u64().unwrap_or(0));
        if value.get("wall_seconds").is_some() {
            rows.push(value.clone());
        }
        if value.get("storage").is_some() {
            footprint.push(value);
        }
    }
    let summary = json!({"passed":true,"server_stopped_cleanly":true,
        "sampled_peak_rss_bytes":peak,"kernel_rss_high_water_bytes":hwm,"phases":rows,"storage":footprint});
    private_json(&reports.join("summary.json"), &summary)?;
    let has_large = summary["phases"]
        .as_array()
        .ok_or("missing phases")?
        .iter()
        .any(|row| row["phase"] == "large_put");
    let payload = if has_large {
        "**1 GiB (1,073,741,824 bytes)** plus **100 individual 4 KiB objects** and **100 different batched 4 KiB objects**"
    } else {
        "**100 individual 4 KiB objects** and **100 different batched 4 KiB objects**, in a pool enrolled with 4 KiB blocks"
    };
    let object_count = if has_large { 201 } else { 200 };
    let mut markdown = format!(
        "# Restored N1 LINK release benchmark\n\n\
        One native client, loopback, independent XFS/ext4 HDDs, required io_uring. \
        Repository release profile, adaptive controller enabled, request quotas disabled. \
        This shared-workstation run is not a matched old/new comparison or scale qualification.\n\n\
        Payload: {payload}. Downloads stream through a SHA-256 sink; \
        every download checks exact length and hash. All {object_count} objects are permanently retired afterward.\n\n\
        Sampled peak engine RSS: **{:.2} MiB** (50 ms samples). Kernel RSS high-water mark: **{:.2} MiB**. Engine stopped cleanly.\n\n\
        | Phase | Wall seconds | Calls | MiB/s | p50 ms | p95 ms | p99 ms | CPU seconds | Peak RSS MiB |\n\
        | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n",
        peak as f64 / 1048576.0,
        hwm as f64 / 1048576.0
    );
    for row in summary["phases"].as_array().ok_or("missing phases")? {
        let number = |field: &str| row[field].as_f64().unwrap_or(0.0);
        let latency = |field: &str| row["latency_ms"][field].as_f64().unwrap_or(0.0);
        markdown.push_str(&format!(
            "| {} | {:.6} | {} | {:.3} | {:.3} | {:.3} | {:.3} | {:.3} | {:.2} |\n",
            row["phase"].as_str().unwrap_or(""),
            number("wall_seconds"),
            row["operations"],
            number("mib_per_second"),
            latency("p50"),
            latency("p95"),
            latency("p99"),
            number("server_cpu_seconds"),
            number("sampled_peak_rss_bytes") / 1048576.0
        ));
    }
    markdown.push_str("\nBatch upload latency is one call containing 100 fresh objects; per-call percentiles are not per-object latencies. Init/auth/report persistence are excluded from PUT/GET timing; init is reported separately. Requested cold reads use per-shard POSIX_FADV_DONTNEED, without a global cache drop; hardware caches remain possible. Warm reads follow. GET timings include client SHA-256 verification.\n\n| Cohort | Source bytes | Shard logical bytes | Shard allocated bytes | Logical amplification | Allocated amplification |\n| --- | ---: | ---: | ---: | ---: | ---: |\n");
    for row in summary["storage"].as_array().ok_or("missing storage")? {
        let storage = &row["storage"];
        markdown.push_str(&format!(
            "| {} | {} | {} | {} | {:.4}x | {:.4}x |\n",
            row["phase"].as_str().unwrap_or(""),
            storage["source_bytes"],
            storage["logical_shard_bytes"],
            storage["allocated_shard_bytes"],
            storage["logical_amplification"].as_f64().unwrap_or(0.0),
            storage["allocated_amplification"].as_f64().unwrap_or(0.0)
        ));
    }
    markdown.push_str("\nFootprint includes redundancy, framing and shard allocation; directory metadata is excluded. Full sample vectors, process CPU/I/O counters, methodology, exact binary hashes, geometry and controller settings are in measurements.jsonl and provenance.json.\n");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(reports.join("BENCHMARK.md"))?;
    file.write_all(markdown.as_bytes())?;
    file.sync_all()?;
    Ok(())
}
