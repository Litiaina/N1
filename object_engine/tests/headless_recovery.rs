//! Opt-in physical witness using only Rust, the real engine and the native SDK.
//! Owned child processes and newly created directories are the only fault targets.
use cli_core::Credentials;
use n1::settings::config::Config;
use n1_link::{ObjectKey, noa::Command};
use n1_native_client::N1NativeClient;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    process::{Child, Command as Process, Stdio},
    time::{Duration, Instant},
};
use tokio::io::AsyncWriteExt;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
static ENGINE: std::sync::LazyLock<PathBuf> = std::sync::LazyLock::new(|| {
    std::env::var_os("N1_WITNESS_ENGINE_BINARY")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_litiaina-n1")))
});
#[path = "headless_recovery/four_members.rs"]
mod four_members;
#[path = "headless_recovery/upgrade.rs"]
mod upgrade;

fn private_json(path: &Path, value: &impl serde::Serialize) -> Result<()> {
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
struct Engine {
    child: Child,
}
impl Engine {
    fn spawn(config: &Path, log: &Path) -> Result<Self> {
        Self::spawn_binary(&ENGINE, config, log)
    }
    fn spawn_binary(binary: &Path, config: &Path, log: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .mode(0o600)
            .open(log)?;
        Ok(Self {
            child: Process::new(binary)
                .args(["serve", "--config"])
                .arg(config)
                .env("N1_IO_BACKEND", "uring")
                .env("RUST_LOG", "info")
                .stdin(Stdio::null())
                .stdout(file.try_clone()?)
                .stderr(file)
                .spawn()?,
        })
    }
    fn stop(&mut self, signal: i32) -> Result<()> {
        self.signal(signal)?;
        self.wait_exit(signal == libc::SIGTERM)
    }
    fn signal(&mut self, signal: i32) -> Result<()> {
        if self.child.try_wait()?.is_none() {
            // This unreaped child is owned by the witness; never target a PID read from a file.
            if unsafe { libc::kill(self.child.id() as i32, signal) } != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
        }
        Ok(())
    }
    fn wait_exit(&mut self, graceful: bool) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(40);
        loop {
            if let Some(status) = self.child.try_wait()? {
                if graceful && !status.success() {
                    return Err(format!("graceful shutdown failed: {status}").into());
                }
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err("engine shutdown exceeded deadline".into());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
impl Drop for Engine {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn offline(args: &[String]) -> Result<Value> {
    let mut child = Process::new(&*ENGINE)
        .args(args)
        .env("N1_IO_BACKEND", "uring")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let deadline = Instant::now() + Duration::from_secs(180);
    while child.try_wait()?.is_none() {
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err("offline witness command exceeded its deadline".into());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let output = child.wait_with_output()?;
    if !output.status.success() {
        return Err(format!(
            "offline command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}
async fn connect(credentials: &Path, engine: &mut Engine) -> Result<N1NativeClient> {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = engine.child.try_wait()? {
            return Err(format!("engine exited before readiness: {status}").into());
        }
        match Credentials::load(credentials)?.connect(None, None).await {
            Ok(client) => return Ok(client),
            Err(error) if Instant::now() >= deadline => return Err(error),
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}
fn value(response: n1_link::noa::Response) -> Result<Value> {
    response
        .result
        .ok_or_else(|| "missing native response".into())
}
fn text(value: &Value, field: &str) -> Result<String> {
    Ok(value[field]
        .as_str()
        .ok_or_else(|| format!("missing {field}"))?
        .into())
}
async fn init(client: &mut N1NativeClient, bytes: u64, part: u64) -> Result<Value> {
    value(
        client
            .control(Command::UploadInit {
                pool: None,
                total_bytes: bytes,
                part_bytes: part,
                durable: true,
                ttl_seconds: 86400,
            })
            .await?,
    )
}
async fn verify(client: &mut N1NativeClient, key: String, expected: &[u8]) -> Result<()> {
    let mut data = Vec::new();
    client.download(key, 0, None, &mut data).await?;
    if data != expected {
        return Err("download differs from exact client bytes".into());
    }
    Ok(())
}
fn free_port() -> Result<String> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .to_string())
}
fn phase(report: &mut Vec<Value>, name: &str, start: Instant) {
    eprintln!("physical witness phase passed: {name}");
    report.push(json!({"phase":name,"passed":true,"seconds":start.elapsed().as_secs_f64()}));
}

/// Prove payload reached every staged inode before interrupting a live source.
/// With 64 KiB blocks and four-stripe groups, 128 KiB only fills the encoder;
/// 512 KiB crosses direct-I/O submissions. Validate bytes, not a fixed sleep.
async fn staged_payload_written(config: &Config, key: &str) -> Result<()> {
    use std::os::unix::fs::FileExt;
    let fp = ObjectKey::inspect(key)?.fingerprint();
    let pool = &config.pools[0];
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let written = (0..pool.layout.width()).all(|shard| {
            let checked = (|| -> Result<()> {
                let slot = pool.layout.member_slot(fp, shard as u16)?;
                let file = fs::File::open(pool.roots[slot].join(fp.staging_path()))?;
                let mut bytes = [0; 128];
                file.read_exact_at(&mut bytes, 0)?;
                let header =
                    n1_link::shard::ShardHeader::decode(&bytes, &pool.layout, fp, shard as u16)?;
                let mut frame = vec![0; header.record_bytes() as usize];
                file.read_exact_at(&mut frame, header.frame_offset(0)?)?;
                n1_link::shard::decode_frame(&header, fp, 0, &frame)?;
                Ok(())
            })();
            checked.is_ok()
        });
        if written {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("payload did not reach staged members before fault injection".into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires explicit N1_WITNESS_PROJECT_ROOT and N1_WITNESS_TEST_ROOT on separate HDDs"]
async fn physical_restart_clone_repair_and_service_identity_recovery() -> Result<()> {
    let first = PathBuf::from(std::env::var("N1_WITNESS_PROJECT_ROOT")?).canonicalize()?;
    let second = PathBuf::from(std::env::var("N1_WITNESS_TEST_ROOT")?).canonicalize()?;
    let first_dev = first.metadata()?.dev();
    let second_dev = second.metadata()?.dev();
    if first_dev == second_dev
        || [first_dev, second_dev].contains(&std::env::current_dir()?.metadata()?.dev())
    {
        return Err(
            "physical witness requires two different filesystems outside the workspace device"
                .into(),
        );
    }
    let id = format!("n1-restoration-witness-{}", ulid::Ulid::new());
    let a = first.join(&id);
    let b = second.join(&id);
    for path in [&a, &b] {
        fs::DirBuilder::new().mode(0o700).create(path)?;
    }
    let result = witness(&a, &b).await;
    if let Err(error) = &result {
        private_json(
            &a.join("failure.json"),
            &json!({"error":error.to_string(),"test_directory":a,"other_directory":b}),
        )?;
    }
    result?;
    eprintln!("Rust physical witness reports: {}", a.display());
    Ok(())
}

async fn witness(a: &Path, b: &Path) -> Result<()> {
    let mut report = Vec::new();
    let config_path = a.join("n1.json");
    let credentials = config_path.with_extension("credentials.json");
    let roots = vec![a.join("member-0"), b.join("member-1")];
    for path in &roots {
        fs::create_dir(path)?;
    }
    let output = Process::new(&*ENGINE)
        .args(["init", "--config"])
        .arg(&config_path)
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
        .arg(free_port()?)
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "enrollment failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    let mut config = Config::load(&config_path)?;
    // Quota behavior is covered separately. This witness exercises storage and recovery.
    config.rate_limits.enabled = false;
    fs::write(&config_path, serde_json::to_vec_pretty(&config)?)?;
    let mut server = Engine::spawn(&config_path, &a.join("engine.log"))?;
    let mut client = connect(&credentials, &mut server).await?;
    let health = value(client.control(Command::Health).await?)?;
    private_json(&a.join("health-initial.json"), &health)?;
    if !health.to_string().contains("io_uring") {
        return Err("required io_uring is not reported".into());
    }

    let start = Instant::now();
    let payload: Vec<_> = (0..4 * 1024 * 1024 + 17)
        .map(|i| (i * 197 % 251) as u8)
        .collect();
    let live = init(&mut client, payload.len() as u64, 5 * 1024 * 1024).await?;
    client
        .put(
            text(&live, "object_key")?,
            payload.len() as u64,
            &mut std::io::Cursor::new(&payload),
        )
        .await?;
    let mut small = Vec::new();
    let mut tickets = Vec::new();
    let mut body = Vec::new();
    for index in 0..8u8 {
        let upload = init(&mut client, 4096, 65536).await?;
        tickets.push(text(&upload, "object_key")?);
        body.extend_from_slice(&vec![index; 4096]);
        small.push(upload);
    }
    client
        .put_batch(tickets, body.len() as u64, &mut std::io::Cursor::new(body))
        .await?;
    let multipart = init(&mut client, 3 * 1024 * 1024, 1024 * 1024).await?;
    let ticket = text(&multipart, "object_key")?;
    client
        .write_at(
            ticket.clone(),
            1024 * 1024,
            1024 * 1024,
            &mut std::io::Cursor::new(vec![17; 1024 * 1024]),
        )
        .await?;
    phase(
        &mut report,
        "acknowledged_normal_batch_and_durable_part",
        start,
    );

    let start = Instant::now();
    let mut interrupted = connect(&credentials, &mut server).await?;
    let (mut reader, mut writer) = tokio::io::duplex(65536);
    let upload_ticket = ticket.clone();
    let upload = tokio::spawn(async move {
        interrupted
            .write_at(upload_ticket, 0, 1024 * 1024, &mut reader)
            .await
    });
    writer.write_all(&vec![16; 512 * 1024]).await?;
    staged_payload_written(&config, &ticket).await?;
    server.stop(libc::SIGKILL)?;
    drop(writer);
    drop(client);
    if tokio::time::timeout(Duration::from_secs(5), upload)
        .await??
        .is_ok()
    {
        return Err("interrupted part was incorrectly acknowledged".into());
    }
    server = Engine::spawn(&config_path, &a.join("engine.log"))?;
    client = connect(&credentials, &mut server).await?;
    verify(&mut client, text(&live, "object_key")?, &payload).await?;
    for (index, object) in small.iter().enumerate() {
        verify(
            &mut client,
            text(object, "object_key")?,
            &vec![index as u8; 4096],
        )
        .await?;
    }
    let status = value(
        client
            .control(Command::UploadStatus {
                object_key: ticket.clone(),
                start: 0,
                limit: 16,
            })
            .await?,
    )?;
    if status["missing_parts"] != json!([0, 2]) {
        return Err(format!("unexpected restarted multipart status: {status}").into());
    }
    for part in [0, 2] {
        client
            .write_at(
                ticket.clone(),
                part * 1024 * 1024,
                1024 * 1024,
                &mut std::io::Cursor::new(vec![16 + part as u8; 1024 * 1024]),
            )
            .await?;
    }
    let finalized = value(
        client
            .control(Command::UploadFinalize {
                object_key: ticket.clone(),
            })
            .await?,
    )?;
    let repeated = value(
        client
            .control(Command::UploadFinalize { object_key: ticket })
            .await?,
    )?;
    if finalized["object_key"] != repeated["object_key"]
        || finalized["total_bytes"] != repeated["total_bytes"]
        || repeated["already_finalized"] != true
    {
        return Err("repeated finalization changed object identity or result".into());
    }
    let multipart_bytes: Vec<_> = [16u8, 17, 18]
        .into_iter()
        .flat_map(|v| vec![v; 1024 * 1024])
        .collect();
    verify(
        &mut client,
        text(&multipart, "object_key")?,
        &multipart_bytes,
    )
    .await?;
    phase(
        &mut report,
        "sigkill_restart_exact_data_and_multipart_resume",
        start,
    );

    let start = Instant::now();
    let graceful_bytes = vec![65; 1024 * 1024];
    let graceful = init(&mut client, graceful_bytes.len() as u64, 1024 * 1024).await?;
    let mut streaming = connect(&credentials, &mut server).await?;
    let object_key = text(&graceful, "object_key")?;
    let (mut reader, mut writer) = tokio::io::duplex(65536);
    let writing =
        tokio::spawn(async move { streaming.put(object_key, 1024 * 1024, &mut reader).await });
    writer.write_all(&graceful_bytes[..512 * 1024]).await?;
    staged_payload_written(&config, &text(&graceful, "object_key")?).await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let health = value(client.control(Command::Health).await?)?;
            if health["operations"]["active_operations"]
                .as_u64()
                .unwrap_or(0)
                > 0
            {
                return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    server.signal(libc::SIGTERM)?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    if server.child.try_wait()?.is_some() {
        return Err("engine exited before its admitted upload drained".into());
    }
    writer.write_all(&graceful_bytes[512 * 1024..]).await?;
    drop(writer);
    let response = tokio::time::timeout(Duration::from_secs(15), writing).await???;
    if value(response)?["object_key"] != graceful["object_key"] {
        return Err("graceful upload changed identity".into());
    }
    server.wait_exit(true)?;
    drop(client);
    server = Engine::spawn(&config_path, &a.join("engine.log"))?;
    client = connect(&credentials, &mut server).await?;
    verify(&mut client, text(&graceful, "object_key")?, &graceful_bytes).await?;
    phase(
        &mut report,
        "sigterm_during_admitted_upload_drains_and_preserves_acknowledged_bytes",
        start,
    );

    let start = Instant::now();
    let retired_key = text(&live, "object_key")?;
    client
        .control(Command::ObjectDelete {
            object_key: retired_key.clone(),
        })
        .await?;
    drop(client);
    server.stop(libc::SIGKILL)?;
    let identity = ObjectKey::inspect(&text(&small[0], "object_key")?)?;
    let fp = identity.fingerprint();
    let slot = config.pools[0].layout.member_slot(fp, 0)?;
    let damaged = roots[slot].join(fp.shard_path(0));
    fs::remove_file(&damaged)?;
    server = Engine::spawn(&config_path, &a.join("engine.log"))?;
    client = connect(&credentials, &mut server).await?;
    verify(&mut client, text(&small[0], "object_key")?, &vec![0; 4096]).await?;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let health = value(
            client
                .control(Command::ObjectHealth {
                    object_key: text(&small[0], "object_key")?,
                    deep: true,
                })
                .await?,
        )?;
        if health["status"] == "healthy" && damaged.exists() {
            break;
        }
        if Instant::now() > deadline {
            return Err(format!("read repair did not complete: {health}").into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let error = client
        .control(Command::ObjectStat {
            object_key: retired_key.clone(),
        })
        .await
        .expect_err("retired object must stay gone");
    if !error.to_string().contains("404") {
        return Err(format!("unexpected retirement error: {error}").into());
    }
    drop(client);
    server.stop(libc::SIGTERM)?;
    phase(
        &mut report,
        "restart_permanent_retirement_and_read_repair",
        start,
    );

    let start = Instant::now();
    let copies = vec![b.join("clone-0"), a.join("clone-1")];
    for (slot, target) in copies.iter().enumerate() {
        fs::create_dir(target)?;
        offline(&[
            "pool".into(),
            "clone".into(),
            "--config".into(),
            config_path.display().to_string(),
            "--pool".into(),
            config.pools[0].layout.id.to_string(),
            "--member".into(),
            slot.to_string(),
            "--copy-root".into(),
            target.display().to_string(),
        ])?;
    }
    config.pools[0].roots = copies;
    fs::write(&config_path, serde_json::to_vec_pretty(&config)?)?;
    server = Engine::spawn(&config_path, &a.join("engine.log"))?;
    client = connect(&credentials, &mut server).await?;
    verify(
        &mut client,
        text(&multipart, "object_key")?,
        &multipart_bytes,
    )
    .await?;
    verify(&mut client, text(&graceful, "object_key")?, &graceful_bytes).await?;
    for (index, object) in small.iter().enumerate() {
        verify(
            &mut client,
            text(object, "object_key")?,
            &vec![index as u8; 4096],
        )
        .await?;
    }
    if client
        .control(Command::ObjectStat {
            object_key: retired_key.clone(),
        })
        .await
        .is_ok()
    {
        return Err("clone revived a retired key".into());
    }
    let replacement_pending = init(&mut client, 3 * 65536, 65536).await?;
    let replacement_ticket = text(&replacement_pending, "object_key")?;
    client
        .write_at(
            replacement_ticket.clone(),
            65536,
            65536,
            &mut std::io::Cursor::new(vec![72; 65536]),
        )
        .await?;
    drop(client);
    server.stop(libc::SIGTERM)?;
    phase(
        &mut report,
        "byte_exact_cross_device_clone_and_cutover",
        start,
    );

    let start = Instant::now();
    let replacement = b.join("replacement-0");
    fs::create_dir(&replacement)?;
    config.pools[0].roots[0] = replacement.clone();
    fs::write(&config_path, serde_json::to_vec_pretty(&config)?)?;
    // Only the explicit command may enroll this replacement. A normal startup
    // cannot adopt the blank path. The original fixture member remains intact.
    let replacement_args = vec![
        "pool".into(),
        "replace".into(),
        "--config".into(),
        config_path.display().to_string(),
        "--pool".into(),
        config.pools[0].layout.id.to_string(),
        "--member".into(),
        "0".into(),
    ];
    let unauthorized = Process::new(&*ENGINE).args(&replacement_args).output()?;
    if unauthorized.status.success() || replacement.join(".n1-arc-pool.json").exists() {
        return Err("replacement proceeded without explicit authorization".into());
    }
    let mut authorized = replacement_args;
    authorized.push("--authorize".into());
    let rebuilt = offline(&authorized)?;
    // Eight small objects, the completed multipart and graceful PUT, and the
    // unfinished replacement upload. The purged object contributes no file.
    if rebuilt["reconstructed_shards"] != 11 || rebuilt["files"] != 12 {
        return Err(format!(
            "replacement inventory differs from the exact live/private set: {rebuilt}"
        )
        .into());
    }
    server = Engine::spawn(&config_path, &a.join("engine.log"))?;
    client = connect(&credentials, &mut server).await?;
    verify(
        &mut client,
        text(&multipart, "object_key")?,
        &multipart_bytes,
    )
    .await?;
    verify(&mut client, text(&graceful, "object_key")?, &graceful_bytes).await?;
    let status = value(
        client
            .control(Command::UploadStatus {
                object_key: replacement_ticket.clone(),
                start: 0,
                limit: 16,
            })
            .await?,
    )?;
    if status["missing_parts"] != json!([0, 2]) || status["finalized"] != false {
        return Err(
            format!("replacement changed acknowledged multipart progress: {status}").into(),
        );
    }
    for part in [0, 2] {
        client
            .write_at(
                replacement_ticket.clone(),
                part * 65536,
                65536,
                &mut std::io::Cursor::new(vec![71 + part as u8; 65536]),
            )
            .await?;
    }
    for _ in 0..2 {
        client
            .control(Command::UploadFinalize {
                object_key: replacement_ticket.clone(),
            })
            .await?;
    }
    let replacement_bytes = [vec![71; 65536], vec![72; 65536], vec![73; 65536]].concat();
    verify(
        &mut client,
        text(&replacement_pending, "object_key")?,
        &replacement_bytes,
    )
    .await?;
    for command in [
        Command::ObjectStat {
            object_key: retired_key.clone(),
        },
        Command::UploadFinalize {
            object_key: text(&live, "object_key")?,
        },
    ] {
        if !matches!(client.control(command).await,Err(n1_native_client::NativeClientError::Native{code,..}) if code=="404")
        {
            return Err("reconstruction did not preserve purged-key absence".into());
        }
    }
    let fingerprint = ObjectKey::inspect(&retired_key)?.fingerprint();
    for root in &config.pools[0].roots {
        for shard in 0..config.pools[0].layout.width() {
            if root.join(fingerprint.shard_path(shard as u16)).exists() {
                return Err("replacement recreated a purged identity file".into());
            }
        }
        if root.join(format!(".uploads/{fingerprint}")).exists() {
            return Err("replacement left purged staging residue".into());
        }
    }
    drop(client);
    server.stop(libc::SIGTERM)?;
    phase(
        &mut report,
        "authorized_failed_member_reconstruction_preserves_keys_retirements_and_durable_parts",
        start,
    );

    let start = Instant::now();
    let repo = b.join("repository");
    let key = a.join("backup.key");
    offline(&[
        "backup".into(),
        "keygen".into(),
        "--output".into(),
        key.display().to_string(),
    ])?;
    offline(&[
        "backup".into(),
        "init".into(),
        "--repository".into(),
        repo.display().to_string(),
        "--key-file".into(),
        key.display().to_string(),
    ])?;
    let point = offline(&[
        "backup".into(),
        "create".into(),
        "--config".into(),
        config_path.display().to_string(),
        "--repository".into(),
        repo.display().to_string(),
        "--key-file".into(),
        key.display().to_string(),
    ])?;
    if point["service_control"] != true {
        return Err("recovery point lacks encrypted service identity".into());
    }
    let recovered_config = a.join("recovered.json");
    let recovered_roots = [a.join("recovered-0"), b.join("recovered-1")];
    for path in &recovered_roots {
        fs::create_dir(path)?;
    }
    offline(&[
        "backup".into(),
        "restore".into(),
        "--repository".into(),
        repo.display().to_string(),
        "--key-file".into(),
        key.display().to_string(),
        "--snapshot".into(),
        text(&point, "id")?,
        "--target".into(),
        recovered_roots[0].display().to_string(),
        "--target".into(),
        recovered_roots[1].display().to_string(),
        "--output-config".into(),
        recovered_config.display().to_string(),
    ])?;
    server = Engine::spawn(&recovered_config, &a.join("recovered-engine.log"))?;
    client = connect(&credentials, &mut server).await?;
    verify(
        &mut client,
        text(&multipart, "object_key")?,
        &multipart_bytes,
    )
    .await?;
    verify(&mut client, text(&graceful, "object_key")?, &graceful_bytes).await?;
    verify(
        &mut client,
        text(&replacement_pending, "object_key")?,
        &replacement_bytes,
    )
    .await?;
    for (index, object) in small.iter().enumerate() {
        verify(
            &mut client,
            text(object, "object_key")?,
            &vec![index as u8; 4096],
        )
        .await?;
    }
    if client
        .control(Command::ObjectStat {
            object_key: retired_key,
        })
        .await
        .is_ok()
    {
        return Err("restore revived a retired key".into());
    }
    let failed = client
        .put(
            text(&live, "object_key")?,
            payload.len() as u64,
            &mut std::io::Cursor::new(&payload),
        )
        .await;
    if failed.is_ok() {
        return Err("restored old upload ticket revived a retired key".into());
    }
    drop(client);
    server.stop(libc::SIGTERM)?;
    phase(
        &mut report,
        "encrypted_backup_and_recovered_service_keep_original_keys_and_retirements",
        start,
    );
    private_json(
        &a.join("result.json"),
        &json!({
            "passed": true,
            "implementation": "Rust process harness + native SDK",
            "engine_version": env!("CARGO_PKG_VERSION"),
            "engine_sha256": format!("{:x}",Sha256::digest(fs::read(&*ENGINE)?)),
            "engine_binary":*ENGINE,
            "initial_shard_format": config.pools[0].layout.shard_format,
            "project_device": a.metadata()?.dev(),
            "test_device": b.metadata()?.dev(),
            "io_backend": "required io_uring",
            "phases": report,
            "data_directories": [a,b]
        }),
    )?;
    Ok(())
}
