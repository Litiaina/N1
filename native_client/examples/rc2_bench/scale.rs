//! Headless physical population witness. Client-held manifests stay outside N1;
//! payloads and keys in flight are bounded by workers * batch size.
use super::supervisor::{Owned, private_json, wait};
use super::{HashSink, Phase, Result, field, process, value};
use n1_native_client::{Command, N1NativeClient};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::os::fd::AsRawFd;
use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    process::{Command as Process, Stdio},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

fn client_lease(root: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(root.join(".qualification.lock"))?;
    if !file.metadata()?.is_file() {
        return Err("invalid qualification lock".into());
    }
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err("scale fixture already has an active qualification client".into());
    }
    Ok(file)
}

const BATCH: u64 = 256;
const WORKERS: u64 = 4;
const SIZES: [usize; 8] = [0, 1, 127, 1024, 4096, 8193, 32768, 65536];
#[path = "scale/resume.rs"]
mod resume;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Entry {
    index: u64,
    key: String,
    ticket: String,
    bytes: u64,
    sha256: String,
}

// Same deterministic SHA-256 seed/repetition generator as the original Rust
// release-proof utility. Each index has its own seed and boundary-sized payload.
fn payload(index: u64) -> Vec<u8> {
    let size = SIZES[index as usize % SIZES.len()];
    let seed = Sha256::digest(index.to_be_bytes());
    seed.iter().copied().cycle().take(size).collect()
}

struct Journal(File);
impl Journal {
    fn new(path: &Path) -> Result<Self> {
        Ok(Self(
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(path)?,
        ))
    }
    fn batch(&mut self, entries: &[Entry]) -> Result<()> {
        let mut bytes = Vec::new();
        for entry in entries {
            serde_json::to_writer(&mut bytes, entry)?;
            bytes.push(b'\n');
        }
        self.0.write_all(&bytes)?;
        self.0.sync_data()?;
        Ok(())
    }
    async fn persist(mut self, entries: Vec<Entry>) -> Result<(Self, Vec<Entry>)> {
        tokio::task::spawn_blocking(move || {
            self.batch(&entries)?;
            Ok((self, entries))
        })
        .await?
    }
}

#[derive(Clone)]
struct Login {
    endpoint: String,
    fragment: String,
    secret: String,
}
impl Login {
    async fn connect(&self) -> Result<N1NativeClient> {
        let mut client = N1NativeClient::connect(&self.endpoint).await?;
        client
            .control(Command::Login {
                fragment: self.fragment.clone(),
                secret: self.secret.clone(),
                permissions: None,
                ttl_seconds: Some(86400),
            })
            .await?;
        Ok(client)
    }
}

fn spawn(binary: &Path, config: &Path, log: &Path) -> Result<Owned> {
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(log)?;
    Ok(Owned(
        Process::new(binary)
            .args(["serve", "--config"])
            .arg(config)
            .env("N1_IO_BACKEND", "uring")
            .env("RUST_LOG", "info")
            .stdin(Stdio::null())
            .stdout(file.try_clone()?)
            .stderr(file)
            .spawn()?,
    ))
}
async fn ready(login: &Login, server: &mut Owned) -> Result<N1NativeClient> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(status) = server.0.try_wait()? {
            return Err(format!("owned scale server exited: {status}").into());
        }
        match login.connect().await {
            Ok(client) => return Ok(client),
            Err(error) if Instant::now() >= deadline => return Err(error),
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}

pub async fn run(args: Vec<String>) -> Result<()> {
    if args.get(1).is_some_and(|v| v == "--scale-resume") {
        return resume::run(args).await;
    }
    if args.get(1).is_some_and(|v| v == "--scale-test-resume") {
        return resume::self_test(args).await;
    }
    if cfg!(debug_assertions) || args.len() != 5 {
        return Err(
            "usage (release client): rc2_bench --scale[‑smoke] engine PROJECT-parent TEST-parent"
                .into(),
        );
    }
    let binary = PathBuf::from(&args[2]).canonicalize()?;
    let project = PathBuf::from(&args[3]).canonicalize()?;
    let test = PathBuf::from(&args[4]).canonicalize()?;
    if project != Path::new("/mnt/PROJECT-DISK-1TB")
        || test != Path::new("/media/altear/TEST-1TB/mx-tests")
        || project.metadata()?.dev() == test.metadata()?.dev()
    {
        return Err(
            "scale witness requires the explicitly selected two independent HDD parents".into(),
        );
    }
    let count = if args[1] == "--scale-smoke" {
        4099
    } else {
        500_000
    };
    let name = format!(
        "n1-scale-{count}-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let a = project.join(&name);
    let b = test.join(&name);
    for path in [&a, &b] {
        fs::DirBuilder::new().mode(0o700).create(path)?;
    }
    println!("headless scale evidence: {}", a.display());
    let result = execute(&binary, &a, &b, count).await;
    if let Err(error) = &result {
        private_json(
            &a.join("FAILED.json"),
            &json!({"error":error.to_string(),"passed":false}),
        )?;
    }
    result
}

async fn execute(binary: &Path, a: &Path, b: &Path, count: u64) -> Result<()> {
    let _lease = client_lease(a)?;
    let config = a.join("service.json");
    let roots = [a.join("member-0"), b.join("member-1")];
    for root in &roots {
        fs::create_dir(root)?;
    }
    let address = std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?;
    let log = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(a.join("init.log"))?;
    let mut child = Owned(
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
                "--block-bytes",
                "65536",
                "--native-address",
            ])
            .arg(address.to_string())
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .spawn()?,
    );
    if !wait(&mut child.0, 60).await?.success() {
        return Err("scale enrollment failed".into());
    }
    drop(child);
    let mut configuration: Value = serde_json::from_reader(File::open(&config)?)?;
    configuration["rate_limits"]["enabled"] = json!(false);
    fs::write(&config, serde_json::to_vec_pretty(&configuration)?)?;
    let credentials: Value =
        serde_json::from_reader(File::open(config.with_extension("credentials.json"))?)?;
    let login = Login {
        endpoint: field(&credentials, "native_url")?,
        fragment: field(&credentials, "fragment")?,
        secret: field(&credentials, "secret")?,
    };
    private_json(
        &a.join("provenance.json"),
        &json!({
            "engine_sha256":format!("{:x}",Sha256::digest(fs::read(binary)?)),"engine_binary":binary,
            "client_sha256":format!("{:x}",Sha256::digest(fs::read(std::env::current_exe()?)?)),
            "objects":count,"sizes_bytes":SIZES,"distribution":"round robin by immutable client index",
            "workers":WORKERS,"batch_limit":BATCH,"durable":true,"http_enabled":false,"native_endpoint":login.endpoint,
            "pool":configuration["pools"][0]["layout"],"concurrency":configuration["concurrency"],
            "max_operations":configuration["max_operations"],"operation_memory_bytes":configuration["operation_memory_bytes"],
            "rate_limits_enabled":false,"adaptive_controller_enabled":true,"io_backend":"required io_uring",
            "project_device":a.metadata()?.dev(),"test_device":b.metadata()?.dev(),
            "journal":"each batch intents fdatasynced before PUT; acknowledgements fdatasynced after reply; keys retained only by client",
            "verification":"every acknowledged key, length and SHA-256 after SIGKILL/restart; no global cache eviction",
            "timing":"upload wall includes init, source generation, network, durable PUT and client journal barriers; batch PUT latency excludes init/journals",
            "kernel":fs::read_to_string("/proc/sys/kernel/osrelease")?.trim()
        }),
    )?;
    let mut server = spawn(binary, &config, &a.join("upload-engine.log"))?;
    let mut client = ready(&login, &mut server).await?;
    let health = value(client.control(Command::Health).await?)?;
    if !health.to_string().contains("io_uring") {
        return Err("scale server did not report io_uring".into());
    }
    private_json(&a.join("health-before.json"), &health)?;
    drop(client);
    let uploaded = Arc::new(AtomicU64::new(0));
    let source_bytes = Arc::new(AtomicU64::new(0));
    let phase = Phase::start(server.0.id())?;
    let mut tasks = tokio::task::JoinSet::new();
    for worker in 0..WORKERS {
        let (login, directory, done, bytes) = (
            login.clone(),
            a.to_path_buf(),
            uploaded.clone(),
            source_bytes.clone(),
        );
        tasks.spawn(
            async move { upload_worker(login, directory, worker, count, done, bytes).await },
        );
    }
    let mut batch_times = Vec::new();
    monitor(
        &mut tasks,
        server.0.id(),
        &uploaded,
        count,
        "upload",
        a,
        &mut batch_times,
    )
    .await?;
    if uploaded.load(Ordering::Relaxed) != count {
        return Err("acknowledged count mismatch".into());
    }
    let mut upload = phase
        .finish(
            server.0.id(),
            "upload",
            source_bytes.load(Ordering::Relaxed),
            batch_times,
        )
        .await?;
    upload["objects"] = json!(count);
    upload["objects_per_second"] =
        json!(count as f64 / upload["wall_seconds"].as_f64().ok_or("missing duration")?);
    upload["latency_unit"] = json!("durable batch PUT; at most 256 objects per call");
    private_json(&a.join("upload-result.json"), &upload)?;
    resume::check_unique(a, count).await?;
    // This unreaped child belongs to the harness, so its PID cannot be reused.
    if unsafe { libc::kill(server.0.id() as i32, libc::SIGKILL) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let status = wait(&mut server.0, 40).await?;
    drop(server);
    let restart = Instant::now();
    let mut server = spawn(binary, &config, &a.join("verification-engine.log"))?;
    let client = ready(&login, &mut server).await?;
    let restart_seconds = restart.elapsed().as_secs_f64();
    drop(client);
    let verified = Arc::new(AtomicU64::new(0));
    let phase = Phase::start(server.0.id())?;
    let mut tasks = tokio::task::JoinSet::new();
    for worker in 0..WORKERS {
        let (login, directory, done) = (login.clone(), a.to_path_buf(), verified.clone());
        tasks.spawn(async move { verify_worker(login, directory, worker, count, done).await });
    }
    let mut get_times = Vec::new();
    monitor(
        &mut tasks,
        server.0.id(),
        &verified,
        count,
        "verify",
        a,
        &mut get_times,
    )
    .await?;
    if verified.load(Ordering::Relaxed) != count {
        return Err("verified count mismatch".into());
    }
    let mut verification = phase
        .finish(
            server.0.id(),
            "verify_after_restart",
            source_bytes.load(Ordering::Relaxed),
            get_times,
        )
        .await?;
    verification["objects"] = json!(count);
    verification["objects_per_second"] = json!(
        count as f64
            / verification["wall_seconds"]
                .as_f64()
                .ok_or("missing duration")?
    );
    let mut client = login.connect().await?;
    private_json(
        &a.join("health-after.json"),
        &value(client.control(Command::Health).await?)?,
    )?;
    drop(client);
    if unsafe { libc::kill(server.0.id() as i32, libc::SIGTERM) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    if !wait(&mut server.0, 40).await?.success() {
        return Err("scale server final shutdown failed".into());
    }
    private_json(&a.join("verification-result.json"), &verification)?;
    private_json(
        &a.join("PASSED.json"),
        &json!({"passed":true,"durably_acknowledged":count,"verified_after_sigkill":count,"upload":upload,"verification":verification,"restart_seconds":restart_seconds,"crash_status":status.to_string(),"failures":0,"clean_final_shutdown":true,"data_retained":true}),
    )?;
    println!(
        "scale PASS: {count} durable uploads and exact verified GETs after SIGKILL; {}",
        a.display()
    );
    Ok(())
}

async fn upload_worker(
    login: Login,
    a: PathBuf,
    worker: u64,
    count: u64,
    done: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
) -> Result<Vec<f64>> {
    let mut client = login.connect().await?;
    let mut intents = Journal::new(&a.join(format!("intents-{worker}.jsonl")))?;
    let mut acknowledged = Journal::new(&a.join(format!("acknowledged-{worker}.jsonl")))?;
    let mut times = Vec::new();
    for first in (worker * BATCH..count).step_by((WORKERS * BATCH) as usize) {
        let mut entries = Vec::new();
        let mut body = Vec::new();
        for index in first..count.min(first + BATCH) {
            let source = payload(index);
            let init = value(
                client
                    .control(Command::UploadInit {
                        pool: None,
                        total_bytes: source.len() as u64,
                        part_bytes: 65536,
                        durable: true,
                        ttl_seconds: 604800,
                    })
                    .await?,
            )?;
            entries.push(Entry {
                index,
                key: field(&init, "object_key")?,
                ticket: field(&init, "upload_key")?,
                bytes: source.len() as u64,
                sha256: format!("{:x}", Sha256::digest(&source)),
            });
            body.extend_from_slice(&source);
        }
        let (next, entries) = intents.persist(entries).await?;
        intents = next;
        let tickets = entries.iter().map(|entry| entry.ticket.clone()).collect();
        let timer = Instant::now();
        let response = value(
            client
                .put_batch(tickets, body.len() as u64, &mut std::io::Cursor::new(&body))
                .await?,
        )?;
        let objects = response["objects"]
            .as_array()
            .ok_or("batch acknowledgement has no objects")?;
        if objects.len() != entries.len()
            || objects.iter().zip(&entries).any(|(object, entry)| {
                object["object_key"] != entry.key || object["total_bytes"] != entry.bytes
            })
        {
            return Err("batch acknowledgement differs from intended identities".into());
        }
        times.push(timer.elapsed().as_secs_f64());
        let (next, entries) = acknowledged.persist(entries).await?;
        acknowledged = next;
        bytes.fetch_add(body.len() as u64, Ordering::Relaxed);
        done.fetch_add(entries.len() as u64, Ordering::Relaxed);
    }
    Ok(times)
}

async fn verify_worker(
    login: Login,
    a: PathBuf,
    worker: u64,
    count: u64,
    done: Arc<AtomicU64>,
) -> Result<Vec<f64>> {
    let mut client = login.connect().await?;
    let mut rows =
        BufReader::new(File::open(a.join(format!("acknowledged-{worker}.jsonl")))?).lines();
    let mut times = Vec::new();
    let mut verified = Journal::new(&a.join(format!("verified-{worker}.jsonl")))?;
    for first in (worker * BATCH..count).step_by((WORKERS * BATCH) as usize) {
        let mut entries = Vec::new();
        for index in first..count.min(first + BATCH) {
            let entry: Entry =
                serde_json::from_str(&rows.next().ok_or("short acknowledged journal")??)?;
            let expected = payload(index);
            if entry.index != index
                || entry.bytes != expected.len() as u64
                || entry.sha256 != format!("{:x}", Sha256::digest(&expected))
            {
                return Err(format!("client journal mismatch at index {index}").into());
            }
            let mut sink = HashSink::default();
            let timer = Instant::now();
            let (_, bytes) = client
                .download(entry.key.clone(), 0, None, &mut sink)
                .await?;
            times.push(timer.elapsed().as_secs_f64());
            if bytes != entry.bytes
                || sink.bytes != entry.bytes
                || format!("{:x}", sink.hash.finalize()) != entry.sha256
            {
                return Err(format!("GET length or SHA-256 mismatch at index {index}").into());
            }
            entries.push(entry);
        }
        let (next, entries) = verified.persist(entries).await?;
        verified = next;
        done.fetch_add(entries.len() as u64, Ordering::Relaxed);
    }
    if rows.next().is_some() {
        return Err("extra acknowledged journal entries".into());
    }
    Ok(times)
}

async fn monitor(
    tasks: &mut tokio::task::JoinSet<Result<Vec<f64>>>,
    pid: u32,
    done: &AtomicU64,
    total: u64,
    name: &str,
    a: &Path,
    times: &mut Vec<f64>,
) -> Result<()> {
    let start = Instant::now();
    let mut failures = Vec::new();
    let mut interval = tokio::time::interval(Duration::from_secs(20));
    let mut events = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(a.join(format!("{name}-progress.jsonl")))?;
    while !tasks.is_empty() {
        tokio::select! {
            task = tasks.join_next() => {
                match task.ok_or("missing worker")? {
                    Ok(Ok(mut values)) => times.append(&mut values),
                    Ok(Err(error)) => failures.push(error.to_string()),
                    Err(error) => failures.push(error.to_string()),
                }
            }
            _ = interval.tick() => {
                let completed = done.load(Ordering::Relaxed);
                let row = json!({"phase":name,"completed":completed,"total":total,"seconds":start.elapsed().as_secs_f64(),"process":process(pid)?});
                let mut data = serde_json::to_vec(&row)?;
                data.push(b'\n');
                events = tokio::task::spawn_blocking(move || -> Result<File> {
                    events.write_all(&data)?;
                    events.sync_data()?;
                    Ok(events)
                }).await??;
                println!("{name}: {completed}/{total}; {:.1}s",start.elapsed().as_secs_f64());
            }
        }
    }
    if !failures.is_empty() {
        private_json(&a.join(format!("{name}-failures.json")), &json!(failures))?;
        return Err(format!(
            "{} worker(s) failed: {}",
            failures.len(),
            failures.join("; ")
        )
        .into());
    }
    Ok(())
}
