//! Real HDD public metadata/object and browser qualification. Workload, oracle,
//! journaling, process supervision and recovery verification are Rust.
#[path = "noa_qualification/external_console.rs"]
mod external_console;
#[path = "noa_qualification/inventory.rs"]
mod inventory;
#[path = "noa_qualification/setup.rs"]
mod setup;
#[path = "noa_qualification/workload.rs"]
mod workload;
use clap::{Parser, ValueEnum};
use std::path::PathBuf;
#[derive(Clone, Copy, ValueEnum)]
enum Mode {
    Api,
    Console,
    Serve,
}
#[derive(Parser)]
struct Args {
    #[arg(long)]
    root: PathBuf,
    /// Optional first-member HDD parent, keeping control/database/journals on
    /// --root (for example DATA_SSD) independently of both object HDDs.
    #[arg(long)]
    first_root: Option<PathBuf>,
    #[arg(long)]
    second_root: PathBuf,
    #[arg(long)]
    object_binary: PathBuf,
    #[arg(long)]
    metadata_binary: PathBuf,
    #[arg(long, default_value_t = 500000)]
    count: u64,
    #[arg(long, value_enum, default_value = "api")]
    mode: Mode,
    #[arg(long, default_value_t = 4)]
    writers: usize,
    #[arg(long, default_value_t = 8)]
    readers: usize,
    /// Independent metadata admission from its existing INI control. The
    /// object engine keeps its own limit and durability/deadline settings.
    #[arg(long)]
    metadata_max_concurrent_io: Option<usize>,
    /// Startup/enrollment budget for each real HDD-backed service, not an I/O
    /// operation timeout or a change to acknowledgement durability.
    #[arg(long, default_value_t = 180)]
    startup_timeout_seconds: u64,
    #[arg(long, default_value = "/home/altear/Development/console")]
    console_repo: PathBuf,
    #[arg(long, default_value = "/tmp/n1-console-tools/bin/node")]
    node: PathBuf,
    #[arg(long)]
    console_driver: Option<PathBuf>,
    #[arg(long, default_value = "http://127.0.0.1:4173")]
    console_url: String,
    #[arg(long)]
    console_dist: Option<PathBuf>,
    /// Drive the real console on another host; this supervisor retains all
    /// server ownership, crash injection, census and full GET verification.
    #[arg(long)]
    external_browser: bool,
}
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    if let Err(e) = run(Args::parse()).await {
        eprintln!("qualification failed: {e}");
        let mut source = e.source();
        for _ in 0..8 {
            let Some(error) = source else {
                break;
            };
            eprintln!("caused by: {error}");
            source = error.source();
        }
        std::process::exit(1);
    }
}
async fn run(args: Args) -> Result<()> {
    if args.count == 0
        || args.count > 10000000
        || !(1..=64).contains(&args.writers)
        || !(1..=128).contains(&args.readers)
        || args
            .metadata_max_concurrent_io
            .is_some_and(|value| !(1..=4096).contains(&value))
        || !(30..=600).contains(&args.startup_timeout_seconds)
        || (args.external_browser && !matches!(args.mode, Mode::Console))
    {
        return Err("invalid qualification bounds".into());
    }
    let mut stack = setup::Stack::open(&args).await?;
    let prefix = match args.mode {
        Mode::Api => "api",
        Mode::Console => "console",
        Mode::Serve => "live",
    };
    let campaign = serde_json::json!({
        "format": 1,
        "mode": prefix,
        "external_browser": args.external_browser,
        "count": args.count,
        "writers": args.writers,
        "readers": args.readers,
        "metadata_max_concurrent_io": args.metadata_max_concurrent_io,
        "payload": "u32-xor-v1-eight-sizes"
    });
    let campaign_path = args.root.join("qualification-campaign.json");
    if campaign_path.exists() {
        if serde_json::from_slice::<serde_json::Value>(&std::fs::read(&campaign_path)?)? != campaign
        {
            return Err("resume parameters do not match this qualification campaign".into());
        }
    } else {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&campaign_path)?;
        file.write_all(&serde_json::to_vec_pretty(&campaign)?)?;
        file.sync_all()?;
    }
    let api = workload::Api::open(&stack.config, prefix).await?;
    let mut samples = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(args.root.join(format!("{prefix}-resources.jsonl")))?;
    let mut object_pid = stack.object.as_ref().unwrap().0.id();
    let mut metadata_pid = stack.metadata.as_ref().unwrap().0.id();
    let ready_path = args.root.join("stack-ready.json");
    let monitor_stop = tokio_util::sync::CancellationToken::new();
    let stop = monitor_stop.clone();
    let monitor = tokio::spawn(async move {
        use std::io::Write;
        loop {
            if let Ok(bytes) = std::fs::read(&ready_path)
                && let Ok(ready) = serde_json::from_slice::<serde_json::Value>(&bytes)
            {
                object_pid = ready["object_pid"]
                    .as_u64()
                    .unwrap_or(u64::from(object_pid)) as u32;
                metadata_pid = ready["metadata_pid"]
                    .as_u64()
                    .unwrap_or(u64::from(metadata_pid)) as u32;
            }
            let values = [
                ("object", object_pid),
                ("metadata", metadata_pid),
                ("supervisor", std::process::id()),
            ]
            .into_iter()
            .map(|(name, pid)| {
                let status =
                    std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default();
                let rss = status
                    .lines()
                    .find(|l| l.starts_with("VmRSS:"))
                    .and_then(|l| l.split_whitespace().nth(1))
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0);
                serde_json::json!({
                    "role": name,
                    "pid": pid,
                    "rss_kib": rss,
                    "stat": std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default()
                })
            })
            .collect::<Vec<_>>();
            let _ = writeln!(
                samples,
                "{}",
                serde_json::json!({"unix":m1::middleware::auth::now(),"processes":values})
            );
            let _ = samples.flush();
            tokio::select! {_=stop.cancelled()=>break,_=tokio::time::sleep(std::time::Duration::from_secs(1))=>()}
        }
    });
    let start = std::time::Instant::now();
    let population = match args.mode {
        Mode::Api => workload::populate(api.clone(), &args.root, args.count, args.writers).await?,
        Mode::Console if args.external_browser => {
            external_console::run(&args, &api, &stack.config.url).await?
        }
        Mode::Console => {
            let driver = args
                .console_driver
                .as_ref()
                .ok_or("--console-driver is required")?;
            let auth = api.login().await?;
            let dist = args
                .console_dist
                .as_ref()
                .ok_or("--console-dist is required")?;
            let origin = reqwest::Url::parse(&args.console_url)?;
            if origin.scheme() != "http" || origin.host_str() != Some("127.0.0.1") {
                return Err("local console qualification requires a loopback HTTP origin".into());
            }
            let mut host = tokio::process::Command::new(&args.node)
                .arg(args.console_repo.join("scripts/serve-console.mjs"))
                .arg("--root")
                .arg(dist)
                .arg("--n1")
                .arg(&stack.config.url)
                .arg("--http-port")
                .arg(
                    origin
                        .port_or_known_default()
                        .ok_or("console port missing")?
                        .to_string(),
                )
                .kill_on_drop(true)
                .spawn()?;
            let http = reqwest::Client::new();
            let mut ready = false;
            for _ in 0..300 {
                if let Some(status) = host.try_wait()? {
                    return Err(format!("console host exited: {status}").into());
                }
                if http
                    .get(format!("{}/healthz", args.console_url))
                    .send()
                    .await
                    .is_ok_and(|r| r.status().is_success())
                {
                    ready = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            if !ready {
                return Err("console host did not become ready".into());
            }
            let mut child = tokio::process::Command::new(&args.node)
                .arg(driver)
                .env("N1_QUAL_CONSOLE_REPO", &args.console_repo)
                .env("N1_QUAL_CONSOLE_URL", &args.console_url)
                .env("N1_QUAL_API_URL", &stack.config.url)
                .env("N1_QUAL_AUTH", auth.to_string())
                .env("N1_QUAL_FRAGMENT", &api.fragment)
                .env("N1_QUAL_COUNT", args.count.to_string())
                .env("N1_QUAL_ROOT", &args.root)
                .kill_on_drop(true)
                .spawn()?;
            if !child.wait().await?.success() {
                return Err("browser qualification failed; inspect its retained artifacts".into());
            }
            host.kill().await?;
            let _ = host.wait().await;
            let report: serde_json::Value = serde_json::from_slice(&std::fs::read(
                args.root.join("console-browser-PASSED.json"),
            )?)?;
            let prior = report["priorAcknowledged"]
                .as_u64()
                .ok_or("browser prior ACK count missing")?;
            serde_json::json!({
                "prior_acknowledged": prior,
                "acknowledged_this_invocation": args.count-prior,
                "total_acknowledged": args.count
            })
        }
        Mode::Serve => {
            std::fs::write(
                args.root.join("live-client.json"),
                serde_json::to_vec_pretty(&serde_json::json!({
                    "url": stack.config.url,
                    "native_port": stack.config.metadata_native,
                    "fragment": api.fragment,
                    "fragment_key": api.secret,
                    "auth": api.login().await?
                }))?,
            )?;
            eprintln!(
                "stack ready; private client configuration: {}",
                args.root.join("live-client.json").display()
            );
            tokio::signal::ctrl_c().await?;
            monitor_stop.cancel();
            monitor.await?;
            return Ok(());
        }
    };
    let upload_seconds = start.elapsed().as_secs_f64();
    // Kill both actual processes. Every verification below crosses freshly opened
    // metadata and object engines; acknowledged objects are never re-uploaded.
    drop(stack.metadata.take());
    drop(stack.object.take());
    let config = stack.config.clone();
    let alias = api.fragment.clone();
    let count = args.count;
    let mode = prefix.to_owned();
    let inventory =
        tokio::task::spawn_blocking(move || inventory::census(&config, &alias, &mode, count))
            .await??;
    stack.restart().await?;
    // Reauthenticate against the recovered namespace without provisioning a
    // fragment. Verification must not recreate state lost during the crash.
    let api = workload::Api::attach(&stack.config, prefix).await?;
    let verification =
        workload::verify(api.clone(), &args.root, args.count, args.readers, prefix).await?;
    monitor_stop.cancel();
    monitor.await?;
    let acknowledged = population["acknowledged_this_invocation"]
        .as_u64()
        .ok_or("population ACK count missing")?;
    let report = serde_json::json!({
        "passed": true,
        "scope": format!("{prefix} -> metadata -> native LINK, two HDDs, process crash/reopen and every object verified"),
        "count": args.count,
        "logical_bytes": workload::logical_bytes(args.count),
        "upload_seconds": upload_seconds,
        "objects_per_second": acknowledged as f64/upload_seconds,
        "throughput_basis": "ACKs obtained in this invocation, including verified uncertain prior uploads",
        "population": population,
        "inventory": inventory,
        "verification": verification,
        "root": args.root,
        "second_root": args.second_root,
        "object_binary_sha256": stack.config.object_sha256,
        "metadata_binary_sha256": stack.config.metadata_sha256,
        "harness_sha256": stack.config.harness_sha256
    });
    std::fs::write(
        args.root.join(format!("{prefix}-PASSED.json")),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!("{report}");
    Ok(())
}
