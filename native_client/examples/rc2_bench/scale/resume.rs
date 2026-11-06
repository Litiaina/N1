//! Resume a client interruption without rewriting acknowledged objects or
//! changing the enrolled engine/pool. Original journals remain byte-for-byte.
use super::*;
#[path = "resume/journals.rs"]
mod journals;
#[path = "resume/smoke.rs"]
mod smoke;
pub(super) async fn self_test(args: Vec<String>) -> Result<()> {
    smoke::self_test(args).await
}
pub(super) async fn check_unique(directory: &Path, total: u64) -> Result<()> {
    let output = directory.to_path_buf();
    let census = tokio::task::spawn_blocking(move || journals::unique(&output, total)).await??;
    private_json(&directory.join("distinct-identities.json"), &census)
}

#[derive(Default)]
struct Counters {
    done: AtomicU64,
    bytes: AtomicU64,
    retried: AtomicU64,
    already_finalized: AtomicU64,
}

pub(super) async fn run(args: Vec<String>) -> Result<()> {
    if cfg!(debug_assertions) || args.len() != 4 {
        return Err(
            "usage (release): rc2_bench --scale-resume engine original-run-directory".into(),
        );
    }
    let binary = PathBuf::from(&args[2]).canonicalize()?;
    let root = PathBuf::from(&args[3]).canonicalize()?;
    if root.parent() != Some(Path::new("/mnt/PROJECT-DISK-1TB"))
        || !root
            .file_name()
            .is_some_and(|v| v.to_string_lossy().starts_with("n1-scale-"))
    {
        return Err("resume requires a private scale fixture directly on PROJECT HDD".into());
    }
    let _lease = client_lease(&root)?;
    let original: Value = serde_json::from_reader(File::open(root.join("provenance.json"))?)?;
    let engine_sha = format!("{:x}", Sha256::digest(fs::read(&binary)?));
    if original["engine_sha256"] != engine_sha
        || original["workers"] != WORKERS
        || original["batch_limit"] != BATCH
    {
        return Err("resume engine/workload differs from original immutable campaign".into());
    }
    let count = original["objects"].as_u64().ok_or("missing object count")?;
    if ![1027, 4099, 500_000].contains(&count) {
        return Err("unknown scale fixture workload".into());
    }
    let config = root.join("service.json");
    let configuration: Value = serde_json::from_reader(File::open(&config)?)?;
    let roots: Vec<PathBuf> = serde_json::from_value(configuration["pools"][0]["roots"].clone())?;
    let second = Path::new("/media/altear/TEST-1TB/mx-tests")
        .join(root.file_name().ok_or("invalid root")?)
        .join("member-1");
    if roots != [root.join("member-0"), second]
        || roots[0].metadata()?.dev() == roots[1].metadata()?.dev()
        || original["project_device"] != roots[0].metadata()?.dev()
        || original["test_device"] != roots[1].metadata()?.dev()
        || configuration["pools"][0]["layout"] != original["pool"]
    {
        return Err("resume pool/device binding differs from the original campaign".into());
    }
    let mut source = root.clone();
    let mut candidates = Vec::new();
    for entry in fs::read_dir(&root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir()
            && entry.file_name().to_string_lossy().starts_with("resume-")
            && entry.path().join("resume-ready.json").is_file()
        {
            candidates.push(entry.path());
        }
    }
    candidates.sort();
    if let Some(latest) = candidates.last() {
        let ready: Value = serde_json::from_reader(File::open(latest.join("resume-ready.json"))?)?;
        if ready["original_root"] != root.to_string_lossy().as_ref()
            || ready["engine_sha256"] != engine_sha
        {
            return Err("resume checkpoint belongs to a different campaign".into());
        }
        source = latest.clone();
    }
    let directory = root.join(format!(
        "resume-{}-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
        std::process::id()
    ));
    fs::DirBuilder::new().mode(0o700).create(&directory)?;
    println!("resuming scale evidence: {}", directory.display());
    let result = execute(
        &binary,
        &root,
        &config,
        &source,
        &directory,
        count,
        &engine_sha,
    )
    .await;
    if let Err(error) = &result {
        private_json(
            &directory.join("FAILED.json"),
            &json!({"passed":false,"error":error.to_string()}),
        )?;
    }
    result
}

async fn execute(
    binary: &Path,
    root: &Path,
    config: &Path,
    source: &Path,
    directory: &Path,
    count: u64,
    engine_sha: &str,
) -> Result<()> {
    let preparation = Instant::now();
    let mut snapshots = Vec::new();
    for worker in 0..WORKERS {
        let (input, output) = (source.to_path_buf(), directory.to_path_buf());
        snapshots.push(
            tokio::task::spawn_blocking(move || journals::snapshot(&input, &output, worker, count))
                .await??,
        );
    }
    let preserved = snapshots
        .iter()
        .map(|s| s.acknowledged.records)
        .sum::<u64>();
    let original_bytes = snapshots
        .iter()
        .map(|s| s.acknowledged.source_bytes)
        .sum::<u64>();
    private_json(
        &directory.join("resume-ready.json"),
        &json!({
            "original_root": root,
            "source_checkpoint": source,
            "engine_sha256": engine_sha,
            "client_sha256": format!("{:x}",Sha256::digest(fs::read(std::env::current_exe()?)?)),
            "objects": count,
            "preserved_acknowledged": preserved,
            "prefixes": snapshots,
            "preparation_seconds": preparation.elapsed().as_secs_f64(),
            "policy": "acknowledged prefix is read-only; unacknowledged intents reuse the exact ticket; original files never modified",
            "timing": "initial interrupted phase has sampled timing only; resume metrics describe remaining PUT work, not an uninterrupted full-campaign benchmark",
            "client_journals": "fdatasync on bounded blocking workers; no persistence I/O on Tokio runtime threads"
        }),
    )?;
    File::open(directory)?.sync_all()?;
    File::open(root)?.sync_all()?;
    println!("validated preserved acknowledgements: {preserved}/{count}");
    let credentials: Value =
        serde_json::from_reader(File::open(config.with_extension("credentials.json"))?)?;
    let login = Login {
        endpoint: field(&credentials, "native_url")?,
        fragment: field(&credentials, "fragment")?,
        secret: field(&credentials, "secret")?,
    };
    let mut server = spawn(binary, config, &directory.join("resumed-upload-engine.log"))?;
    let mut client = ready(&login, &mut server).await?;
    let health = value(client.control(Command::Health).await?)?;
    if !health.to_string().contains("io_uring") {
        return Err("resume lost required io_uring".into());
    }
    private_json(&directory.join("health-before.json"), &health)?;
    drop(client);
    let progress = Arc::new(Counters::default());
    progress.done.store(preserved, Ordering::Relaxed);
    let phase = Phase::start(server.0.id())?;
    let mut tasks = tokio::task::JoinSet::new();
    for (worker, snapshot) in snapshots.into_iter().enumerate() {
        let (login, output, progress) = (login.clone(), directory.to_path_buf(), progress.clone());
        tasks.spawn(async move {
            journals::upload(login, output, worker as u64, count, snapshot, progress).await
        });
    }
    let mut times = Vec::new();
    monitor(
        &mut tasks,
        server.0.id(),
        &progress.done,
        count,
        "resumed-upload",
        directory,
        &mut times,
    )
    .await?;
    if progress.done.load(Ordering::Relaxed) != count {
        return Err("resumed acknowledged count mismatch".into());
    }
    let bytes = progress.bytes.load(Ordering::Relaxed);
    let mut upload = phase
        .finish(server.0.id(), "remaining_upload_workflow", bytes, times)
        .await?;
    upload["newly_acknowledged"] = json!(count - preserved);
    upload["objects_per_second"] =
        json!((count - preserved) as f64 / upload["wall_seconds"].as_f64().ok_or("no duration")?);
    upload["latency_unit"] = json!(
        "durable batch PUT, at most 256 objects; includes response handling; excludes init and own journal barriers"
    );
    private_json(&directory.join("upload-result.json"), &upload)?;
    check_unique(directory, count).await?;
    if unsafe { libc::kill(server.0.id() as i32, libc::SIGKILL) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let status = wait(&mut server.0, 40).await?;
    drop(server);
    let restarting = Instant::now();
    let mut server = spawn(binary, config, &directory.join("verification-engine.log"))?;
    let client = ready(&login, &mut server).await?;
    let restart_seconds = restarting.elapsed().as_secs_f64();
    drop(client);
    let verified = Arc::new(AtomicU64::new(0));
    let phase = Phase::start(server.0.id())?;
    let mut tasks = tokio::task::JoinSet::new();
    for worker in 0..WORKERS {
        let (login, output, done) = (login.clone(), directory.to_path_buf(), verified.clone());
        tasks.spawn(async move { verify_worker(login, output, worker, count, done).await });
    }
    let mut times = Vec::new();
    monitor(
        &mut tasks,
        server.0.id(),
        &verified,
        count,
        "verify",
        directory,
        &mut times,
    )
    .await?;
    if verified.load(Ordering::Relaxed) != count {
        return Err("full resumed verification count mismatch".into());
    }
    let verification = phase
        .finish(
            server.0.id(),
            "all_objects_verified_after_sigkill",
            original_bytes + bytes,
            times,
        )
        .await?;
    let mut client = login.connect().await?;
    private_json(
        &directory.join("health-after.json"),
        &value(client.control(Command::Health).await?)?,
    )?;
    drop(client);
    if unsafe { libc::kill(server.0.id() as i32, libc::SIGTERM) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    if !wait(&mut server.0, 40).await?.success() {
        return Err("resumed final shutdown failed".into());
    }
    private_json(&directory.join("verification-result.json"), &verification)?;
    private_json(
        &directory.join("PASSED.json"),
        &json!({
            "passed": true,
            "durably_acknowledged": count,
            "verified_after_sigkill": count,
            "preserved_acknowledged_objects_not_reuploaded": preserved,
            "resumed_acknowledgements": count-preserved,
            "unacknowledged_intents_retried": progress.retried.load(Ordering::Relaxed),
            "lost_reply_objects_already_finalized": progress.already_finalized.load(Ordering::Relaxed),
            "upload": upload,
            "verification": verification,
            "restart_seconds": restart_seconds,
            "source_bytes": original_bytes+bytes,
            "crash_status": status.to_string(),
            "failures": 0,
            "clean_final_shutdown": true,
            "original_root": root,
            "source_checkpoint": source,
            "engine_sha256": engine_sha
        }),
    )?;
    println!(
        "resumed scale PASS: {count} immutable identities and exact verified GETs after SIGKILL; {}",
        directory.display()
    );
    Ok(())
}
