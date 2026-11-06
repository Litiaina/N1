//! Actual operator entry points and native/HTTP requests. INI configuration is
//! tested through a running binary, not just by asserting parsed field values.
use n1_native_client::{Command, N1NativeClient};
use serde_json::Value;
use std::{
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
    path::PathBuf,
    process::{Child, Command as Process, Stdio},
    time::Duration,
};
use tokio::net::TcpListener;

const SECRET: &str = "ini-functional-object-credential";
// Fixtures intentionally stop and rebind their configured ports. Keep those
// port lifetimes separate: a concurrent fixture can otherwise select a port
// during another fixture's restart gap and satisfy the wrong readiness probe.
static FIXTURE_PORTS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
struct Fixture {
    _port_guard: tokio::sync::MutexGuard<'static, ()>,
    _root: tempfile::TempDir,
    root: PathBuf,
    ini: PathBuf,
    native: u16,
    http: u16,
}
struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
async fn port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
impl Fixture {
    async fn new() -> Self {
        let port_guard = FIXTURE_PORTS.lock().await;
        let mut builder = tempfile::Builder::new();
        builder.permissions(fs::Permissions::from_mode(0o700));
        let directory = match std::env::var_os("N1_INI_TEST_HDD_ROOT") {
            Some(parent) => builder.tempdir_in(parent),
            None => builder.tempdir(),
        }
        .unwrap();
        let root = directory.path().to_path_buf();
        let ini = root.join("n1_config.ini");
        let native = port().await;
        let mut http = port().await;
        while http == native {
            http = port().await;
        }
        fs::create_dir(root.join("a0")).unwrap();
        let config = format!(
            concat!(
                "[server]\n",
                "host=127.0.0.1\n",
                "https_port={http}\n",
                "native_port={native}\n",
                "native_max_connections=7\n",
                "cert_path=\n",
                "key_path=\n",
                "[disk_config]\n",
                "fs_storage_path={}\n",
                "[storage_pool.A]\n",
                "generation=1\n",
                "roots={}\n",
                "data_shards=1\n",
                "parity_shards=0\n",
                "sealed=false\n",
                "[upload_config]\n",
                "max_concurrent_io=3\n",
                "adaptive_concurrency_enabled=false\n",
                "adaptive_concurrency_interval_ms=1500\n",
                "writer_buffer_size=4096\n",
                "upload_memory_budget_bytes=67108864\n",
                "stream_max_concurrency=4\n",
                "stream_chunk_size=65536\n",
                "stream_readahead_bytes=131072\n",
                "stream_locality_bias_enabled=false\n",
                "upload_io_gate_enabled=false\n",
                "upload_part_strict_durability_default=true\n",
                "max_part_size=16384\n",
                "stripe_block_size=4096\n",
                "[io_control]\n",
                "foreground_wait_timeout_ms=1100\n",
                "stall_threshold_ms=1500\n",
                "slow_io_threshold_ms=20\n",
                "latency_samples_per_class=128\n",
                "[set_limits]\n",
                "max_file_size=1048576\n",
                "[rate_limiter_config]\n",
                "enable_rate_limiting=false\n",
                "[garbage_collector]\n",
                "cleanup_interval_seconds=60\n",
                "file_ttl_seconds=60\n",
                "trash_retention_seconds=2592000\n",
                "[auto_healing]\n",
                "enabled=false\n",
                "scrub_schedule_utc=\n",
                "initial_scan_delay_seconds=0\n",
            ),
            root.join("control").display(),
            root.join("a0").display(),
            http = http,
            native = native
        );
        fs::write(&ini, config).unwrap();
        let mut env = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(root.join("n1.env"))
            .unwrap();
        write!(
            env,
            concat!(
                "JWT_SECRET='test-jwt#quoted' # inline comment\n",
                "AUTH_KEY='{SECRET}'\n",
                "MASTER_KEY=test-master\n",
                "ADMIN_KEY=test-admin\n",
                "ACCESS_TOKEN_DEFAULT_TTL_SECS=120\n",
                "ACCESS_TOKEN_MAX_TTL_SECS=300\n",
                "REFRESH_TOKEN_TTL_SECS=600\n",
            ),
            SECRET = SECRET
        )
        .unwrap();
        env.sync_all().unwrap();
        Self {
            _port_guard: port_guard,
            _root: directory,
            root,
            ini,
            native,
            http,
        }
    }
    fn run(&self, action: &str) -> std::process::Output {
        Process::new(env!("CARGO_BIN_EXE_litiaina-n1"))
            .args([action, "--config"])
            .arg(&self.ini)
            .output()
            .unwrap()
    }
    fn edit(&self, change: impl FnOnce(&mut ini::Ini)) {
        let mut ini = ini::Ini::load_from_file(&self.ini).unwrap();
        change(&mut ini);
        ini.write_to_file(&self.ini).unwrap();
    }
    fn registry(&self) -> Value {
        serde_json::from_slice(&fs::read(self.root.join("control/pool-enrollment.json")).unwrap())
            .unwrap()
    }
    fn registry_file(&self) -> PathBuf {
        self.root.join("control/pool-enrollment.json")
    }
    async fn start(&self, host: &str, native: u16) -> Server {
        self.start_with_env(host, native, &[]).await
    }
    async fn start_with_env(
        &self,
        host: &str,
        native: u16,
        environment: &[(&str, &str)],
    ) -> Server {
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join("server.log"))
            .unwrap();
        let mut server = Server(
            Process::new(env!("CARGO_BIN_EXE_litiaina-n1"))
                .args(["--config"])
                .arg(&self.ini)
                .envs(environment.iter().copied())
                .stdout(Stdio::from(log.try_clone().unwrap()))
                .stderr(Stdio::from(log))
                .spawn()
                .unwrap(),
        );
        for _ in 0..1200 {
            if tokio::net::TcpStream::connect((host, native)).await.is_ok() {
                return server;
            }
            assert!(
                server.0.try_wait().unwrap().is_none(),
                "{}",
                fs::read_to_string(self.root.join("server.log")).unwrap()
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!(
            "server did not bind configured IP/port within 30 seconds: {}",
            fs::read_to_string(self.root.join("server.log")).unwrap_or_default()
        );
    }
    async fn client(&self, host: &str, native: u16, tls: bool) -> N1NativeClient {
        let ca = if tls {
            Some(cli_core::client_tls(&self.root.join("cert.pem")).unwrap())
        } else {
            None
        };
        let mut client = N1NativeClient::connect_with_tls(
            &format!("{}://{host}:{native}", if tls { "n1s" } else { "n1" }),
            ca,
        )
        .await
        .unwrap();
        let login = client
            .auth_fragment("root", SECRET)
            .await
            .unwrap()
            .result
            .unwrap();
        assert_eq!(
            login["session"]["exp"].as_u64().unwrap() - login["session"]["iat"].as_u64().unwrap(),
            120
        );
        assert_eq!(login["refresh_expires_in"], 600);
        client
    }
}
async fn put(client: &mut N1NativeClient, mut bytes: &[u8]) -> Value {
    let result = client
        .control(Command::UploadInit {
            pool: None,
            total_bytes: bytes.len() as u64,
            part_bytes: (bytes.len() as u64).max(1).div_ceil(4096) * 4096,
            durable: true,
            ttl_seconds: 3600,
        })
        .await
        .unwrap()
        .result
        .unwrap();
    client
        .put(
            result["object_key"].as_str().unwrap().into(),
            bytes.len() as u64,
            &mut bytes,
        )
        .await
        .unwrap();
    result
}

#[tokio::test]
async fn optional_size_cap_changes_policy_without_changing_identity_or_part_limits() {
    let f = Fixture::new().await;
    let server = f.start("127.0.0.1", f.native).await;
    let mut client = f.client("127.0.0.1", f.native, false).await;
    let bytes = (0..2 * 1024 * 1024 + 7)
        .map(|n| (n % 251) as u8)
        .collect::<Vec<_>>();
    let init = || Command::UploadInit {
        pool: None,
        total_bytes: bytes.len() as u64,
        part_bytes: 16384,
        durable: true,
        ttl_seconds: 3600,
    };
    assert!(client.control(init()).await.is_err());
    let marker = fs::read(f.root.join("a0/.n1-arc-pool.json")).unwrap();
    let signing = fs::read(f.root.join("control/link.signing-key")).unwrap();
    drop(client);
    drop(server);
    f.edit(|ini| {
        ini.with_section(Some("set_limits"))
            .set("max_file_size", "unlimited");
    });
    let server = f.start("127.0.0.1", f.native).await;
    let mut client = f.client("127.0.0.1", f.native, false).await;
    let upload = client.control(init()).await.unwrap().result.unwrap();
    let ticket = upload["object_key"].as_str().unwrap();
    for (index, mut part) in bytes.chunks(16384).enumerate() {
        client
            .write_at(
                ticket.into(),
                index as u64 * 16384,
                part.len() as u64,
                &mut part,
            )
            .await
            .unwrap();
    }
    client
        .control(Command::UploadFinalize {
            object_key: ticket.into(),
        })
        .await
        .unwrap();
    let mut read = Vec::new();
    client
        .download(
            upload["object_key"].as_str().unwrap().into(),
            0,
            None,
            &mut read,
        )
        .await
        .unwrap();
    assert_eq!(read, bytes);
    let oversized = client
        .control(Command::UploadInit {
            pool: None,
            total_bytes: 16385,
            part_bytes: 32768,
            durable: true,
            ttl_seconds: 3600,
        })
        .await
        .unwrap()
        .result
        .unwrap();
    assert!(
        client
            .write_at(
                oversized["object_key"].as_str().unwrap().into(),
                0,
                16385,
                &mut &vec![0; 16385][..]
            )
            .await
            .is_err()
    );
    // A rejected streaming request may close the connection; reauthenticate.
    let mut client = f.client("127.0.0.1", f.native, false).await;
    let health = client
        .control(Command::Health)
        .await
        .unwrap()
        .result
        .unwrap();
    assert!(health["configuration"]["runtime"]["max_file_size"].is_null());
    assert_eq!(health["configuration"]["runtime"]["max_part_size"], 16384);
    assert_eq!(health["configuration"]["operation_memory_bytes"], 67108864);
    assert_eq!(
        marker,
        fs::read(f.root.join("a0/.n1-arc-pool.json")).unwrap()
    );
    assert_eq!(
        signing,
        fs::read(f.root.join("control/link.signing-key")).unwrap()
    );
    drop(client);
    drop(server);
}

#[tokio::test]
async fn object_starts_with_explicit_secrets_or_environment_without_creating_sibling_env() {
    let mut f = Fixture::new().await;
    let renamed = f.root.join("object.ini");
    fs::rename(&f.ini, &renamed).unwrap();
    f.ini = renamed;
    fs::rename(f.root.join("n1.env"), f.root.join("object.env")).unwrap();
    f.edit(|ini| {
        ini.with_section(Some("server"))
            .set("env_file", "object.env");
        ini.with_section(Some("disk_config"))
            .set("fs_storage_path", "control");
        ini.with_section(Some("storage_pool.A")).set("roots", "a0");
    });
    let installer =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../scripts/install-n1-systemd.sh");
    let rendered = Process::new("bash")
        .arg(installer)
        .arg("--install-dir")
        .arg(&f.root)
        .arg("--binary")
        .arg(env!("CARGO_BIN_EXE_litiaina-n1"))
        .arg("--config-file")
        .arg(&f.ini)
        .args(["--no-env-file", "--dry-run"])
        .output()
        .unwrap();
    assert!(
        rendered.status.success(),
        "{}",
        String::from_utf8_lossy(&rendered.stderr)
    );
    let unit = String::from_utf8(rendered.stdout).unwrap();
    assert!(unit.contains("object.ini"));
    assert!(!unit.contains("EnvironmentFile="));
    let mounts = unit
        .lines()
        .find(|line| line.starts_with("RequiresMountsFor="))
        .unwrap();
    assert!(mounts.contains(f.root.join("a0").to_str().unwrap()));
    assert!(mounts.contains(f.root.join("control").to_str().unwrap()));
    let export_unit = || {
        Process::new("bash")
            .arg(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../scripts/install-n1-systemd.sh"),
            )
            .arg("--install-dir")
            .arg(&f.root)
            .arg("--binary")
            .arg(env!("CARGO_BIN_EXE_litiaina-n1"))
            .arg("--config-file")
            .arg(&f.ini)
            .arg("--env-file")
            .arg(f.root.join("object.env"))
            .arg("--dry-run")
            .output()
            .unwrap()
    };
    assert!(export_unit().status.success());
    fs::set_permissions(f.root.join("object.env"), fs::Permissions::from_mode(0o644)).unwrap();
    let insecure = export_unit();
    assert!(!insecure.status.success());
    assert!(String::from_utf8_lossy(&insecure.stderr).contains("owner-only"));
    fs::set_permissions(f.root.join("object.env"), fs::Permissions::from_mode(0o600)).unwrap();
    let server = f.start("127.0.0.1", f.native).await;
    let client = f.client("127.0.0.1", f.native, false).await;
    assert!(!f.root.join("n1.env").exists());
    drop(client);
    drop(server);
    f.edit(|ini| {
        ini.with_section(Some("server")).delete(&"env_file");
    });
    let server = f
        .start_with_env(
            "127.0.0.1",
            f.native,
            &[
                ("JWT_SECRET", "test-service-jwt"),
                ("AUTH_KEY", SECRET),
                ("MASTER_KEY", "test-master"),
                ("ADMIN_KEY", "test-admin"),
                ("ACCESS_TOKEN_DEFAULT_TTL_SECS", "120"),
                ("ACCESS_TOKEN_MAX_TTL_SECS", "300"),
                ("REFRESH_TOKEN_TTL_SECS", "600"),
            ],
        )
        .await;
    let mut client = f.client("127.0.0.1", f.native, false).await;
    let upload = put(&mut client, b"environment-only service credentials").await;
    let mut read = Vec::new();
    client
        .download(
            upload["object_key"].as_str().unwrap().into(),
            0,
            None,
            &mut read,
        )
        .await
        .unwrap();
    assert_eq!(read, b"environment-only service credentials");
    assert!(!f.root.join("n1.env").exists());
    drop(client);
    drop(server);
}

#[tokio::test]
async fn ini_controls_real_runtime_tls_restart_and_complete_pool_addition() {
    let f = Fixture::new().await;
    let repository = f.root.join("repository");
    let backup_key = f.root.join("recovery.key");
    n1::core::backup::create_key(&backup_key).unwrap();
    n1::core::backup::repository::Repository::initialize(&repository, &backup_key).unwrap();
    f.edit(|ini| {
        ini.with_section(Some("backup_repository.archive"))
            .set("path", "repository")
            .set("key_file", "recovery.key");
    });
    assert!(!f.run("check").status.success());
    assert!(!f.root.join("a0/.n1-arc-pool.json").exists());
    let server = f.start("127.0.0.1", f.native).await;
    let mut client = f.client("127.0.0.1", f.native, false).await;
    assert!(
        client
            .control(Command::BackupList {
                repository: "archive".into(),
                after: String::new(),
                limit: 10
            })
            .await
            .is_ok()
    );
    let health = client
        .control(Command::Health)
        .await
        .unwrap()
        .result
        .unwrap();
    let c = &health["configuration"];
    assert_eq!(c["max_connections"], 7);
    assert_eq!(c["max_operations"], 3);
    assert_eq!(c["operation_memory_bytes"], 67108864);
    assert_eq!(c["concurrency"]["adaptive_enabled"], false);
    assert_eq!(c["concurrency"]["max_streams"], 4);
    assert_eq!(
        c["runtime"]["io_control"]["foreground_wait_timeout_ms"],
        1100
    );
    assert_eq!(c["runtime"]["writer_buffer_size"], 4096);
    assert_eq!(c["runtime"]["upload_io_gate_enabled"], false);
    assert_eq!(c["scheduled_maintenance"]["gc"]["every_seconds"], 60);
    assert!(c["scheduled_maintenance"]["calendar_scrub"].is_null());
    assert!(
        reqwest::get(format!("http://127.0.0.1:{}/health", f.http))
            .await
            .unwrap()
            .status()
            .is_success()
    );
    let bytes = (0..65541).map(|n| (n % 251) as u8).collect::<Vec<_>>();
    let first = put(&mut client, &bytes).await;
    assert!(
        client
            .control(Command::UploadInit {
                pool: None,
                total_bytes: 1048577,
                part_bytes: 4096,
                durable: true,
                ttl_seconds: 60
            })
            .await
            .is_err()
    );
    assert!(
        !f.run("init").status.success(),
        "running node must hold the enrollment lease"
    );
    let before = f.registry();
    let marker = fs::read(f.root.join("a0/.n1-arc-pool.json")).unwrap();
    let secret = fs::read(f.root.join("control/link.signing-key")).unwrap();
    drop(client);
    drop(server);
    let check = f.run("check");
    assert!(
        check.status.success(),
        "{}",
        String::from_utf8_lossy(&check.stderr)
    );
    fs::create_dir(f.root.join("b0")).unwrap();
    let new_native = port().await;
    let mut new_http = port().await;
    while new_http == new_native {
        new_http = port().await;
    }
    let certificate = Process::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost,IP:127.0.0.2",
            "-addext",
            "basicConstraints=critical,CA:FALSE",
            "-keyout",
        ])
        .arg(f.root.join("key.pem"))
        .arg("-out")
        .arg(f.root.join("cert.pem"))
        .output()
        .unwrap();
    assert!(certificate.status.success());
    f.edit(|ini| {
        ini.with_section(Some("server"))
            .set("host", "127.0.0.2")
            .set("native_port", new_native.to_string())
            .set("https_port", new_http.to_string())
            .set("cert_path", "cert.pem")
            .set("key_path", "key.pem")
            .set("native_max_connections", "11");
        ini.with_section(Some("upload_config"))
            .set("max_concurrent_io", "5")
            .set("upload_io_gate_enabled", "true")
            .set("stream_locality_bias_enabled", "true");
        ini.with_section(Some("auto_healing"))
            .set("enabled", "true")
            .set("scrub_schedule_utc", "Sun 02:00");
        ini.with_section(Some("storage_pool.B"))
            .set("generation", "1")
            .set("roots", f.root.join("b0").to_str().unwrap())
            .set("data_shards", "1")
            .set("parity_shards", "0")
            .set("sealed", "false");
    });
    assert!(!f.run("check").status.success());
    assert!(!f.root.join("b0/.n1-arc-pool.json").exists());
    let server = f.start("127.0.0.2", new_native).await;
    let mut client = f.client("127.0.0.2", new_native, true).await;
    let health = client
        .control(Command::Health)
        .await
        .unwrap()
        .result
        .unwrap();
    assert_eq!(health["configuration"]["max_operations"], 5);
    assert_eq!(health["configuration"]["max_connections"], 11);
    assert_eq!(
        health["configuration"]["scheduled_maintenance"]["calendar_scrub"]["schedule_utc"],
        "Sun 02:00"
    );
    let certificate =
        reqwest::Certificate::from_pem(&fs::read(f.root.join("cert.pem")).unwrap()).unwrap();
    let https = reqwest::Client::builder()
        .no_proxy()
        .add_root_certificate(certificate)
        .build()
        .unwrap();
    assert!(
        https
            .get(format!("https://127.0.0.2:{new_http}/health"))
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    let mut read = Vec::new();
    client
        .download(
            first["object_key"].as_str().unwrap().into(),
            0,
            None,
            &mut read,
        )
        .await
        .unwrap();
    assert_eq!(read, bytes);
    let pools = client
        .control(Command::Pools)
        .await
        .unwrap()
        .result
        .unwrap();
    assert_eq!(pools["pools"].as_array().unwrap().len(), 2);
    let new = put(&mut client, &bytes).await;
    assert_ne!(first["object_key"], new["object_key"]);
    let after = f.registry();
    assert_eq!(
        before["pools"]["A"]["config"]["layout"],
        after["pools"]["A"]["config"]["layout"]
    );
    assert_eq!(before["principal"], after["principal"]);
    assert_eq!(
        marker,
        fs::read(f.root.join("a0/.n1-arc-pool.json")).unwrap()
    );
    assert_eq!(
        secret,
        fs::read(f.root.join("control/link.signing-key")).unwrap()
    );
    drop(client);
    drop(server);
    let result = f.run("check");
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

#[tokio::test]
async fn invalid_geometry_roots_and_signing_state_never_reenroll_existing_members() {
    let f = Fixture::new().await;
    let init = f.run("init");
    assert!(
        init.status.success(),
        "{}",
        String::from_utf8_lossy(&init.stderr)
    );
    let original = fs::read(&f.ini).unwrap();
    let marker = fs::read(f.root.join("a0/.n1-arc-pool.json")).unwrap();
    let registry = fs::read(f.root.join("control/pool-enrollment.json")).unwrap();
    f.edit(|ini| {
        ini.with_section(Some("storage_pool.A"))
            .set("generation", "2");
    });
    assert!(!f.run("init").status.success());
    fs::write(&f.ini, &original).unwrap();
    fs::rename(f.root.join("a0"), f.root.join("original-member")).unwrap();
    fs::create_dir(f.root.join("a0")).unwrap();
    assert!(!f.run("init").status.success());
    assert!(fs::read_dir(f.root.join("a0")).unwrap().next().is_none());
    fs::remove_dir(f.root.join("a0")).unwrap();
    fs::rename(f.root.join("original-member"), f.root.join("a0")).unwrap();
    assert_eq!(
        marker,
        fs::read(f.root.join("a0/.n1-arc-pool.json")).unwrap()
    );
    assert_eq!(
        registry,
        fs::read(f.root.join("control/pool-enrollment.json")).unwrap()
    );
    let signing = f.root.join("control/link.signing-key");
    let secret = fs::read(&signing).unwrap();
    let mut altered = secret.clone();
    altered[0] ^= 1;
    fs::write(&signing, &altered).unwrap();
    assert!(!f.run("check").status.success());
    assert!(!f.run("init").status.success());
    assert_eq!(fs::read(&signing).unwrap(), altered);
    fs::write(&signing, &secret).unwrap();
    fs::rename(&signing, signing.with_extension("saved")).unwrap();
    assert!(!f.run("init").status.success());
    assert!(!signing.exists());
}

#[tokio::test]
async fn interrupted_planned_enrollment_resumes_exact_ids_and_rejects_foreign_bytes() {
    let f = Fixture::new().await;
    assert!(f.run("init").status.success());
    let root = f.root.join("b0");
    fs::create_dir(&root).unwrap();
    f.edit(|ini| {
        ini.with_section(Some("storage_pool.B"))
            .set("generation", "1")
            .set("roots", root.to_str().unwrap())
            .set("data_shards", "1")
            .set("parity_shards", "0")
            .set("sealed", "false");
    });
    let mut registry = f.registry();
    let mut plan = registry["pools"]["A"].clone();
    plan["config"]["roots"] = serde_json::json!([root]);
    plan["config"]["layout"]["id"] = Value::String("c1".repeat(16));
    plan["config"]["members"] = serde_json::json!(["d2".repeat(16)]);
    plan["ready"] = Value::Bool(false);
    registry["pools"]["B"] = plan;
    fs::write(f.registry_file(), serde_json::to_vec(&registry).unwrap()).unwrap();
    let marker = n1::core::storage_pool::MemberMarker {
        format: "LINK-pool-v1".into(),
        layout: serde_json::from_value(registry["pools"]["B"]["config"]["layout"].clone()).unwrap(),
        members: vec![n1_link::PoolId([0xd2; 16])],
        slot: 0,
    };
    let bytes = serde_json::to_vec(&marker).unwrap();
    fs::write(root.join(".n1-arc-pool.json"), &bytes[..17]).unwrap();
    fs::write(root.join("foreign-data"), b"preserve").unwrap();
    assert!(!f.run("init").status.success());
    assert_eq!(fs::read(root.join("foreign-data")).unwrap(), b"preserve");
    assert_eq!(
        fs::read(root.join(".n1-arc-pool.json")).unwrap(),
        &bytes[..17]
    );
    fs::remove_file(root.join("foreign-data")).unwrap();
    let run = f.run("init");
    assert!(
        run.status.success(),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );
    let registered = f.registry();
    assert_eq!(
        registered["pools"]["B"]["config"]["layout"]["id"],
        "c1".repeat(16)
    );
    assert_eq!(registered["pools"]["B"]["ready"], true);
    let actual: n1::core::storage_pool::MemberMarker =
        serde_json::from_slice(&fs::read(root.join(".n1-arc-pool.json")).unwrap()).unwrap();
    assert_eq!(actual, marker);
    assert!(f.run("check").status.success());
}

#[tokio::test]
async fn removed_json_import_flag_cannot_enroll_or_rebind_members() {
    let f = Fixture::new().await;
    let run = Process::new(env!("CARGO_BIN_EXE_litiaina-n1"))
        .args(["init", "--config"])
        .arg(&f.ini)
        .args(["--from-config", "/nonexistent/development.json"])
        .output()
        .unwrap();
    assert!(!run.status.success());
    assert!(String::from_utf8_lossy(&run.stderr).contains("--from-config"));
    assert!(!f.root.join("a0/.n1-arc-pool.json").exists());
    assert!(!f.root.join("control/pool-enrollment.json").exists());
}

#[tokio::test]
async fn ini_healing_is_opt_in_background_work_and_monitor_controls_are_live() {
    let f = Fixture::new().await;
    let schedule = chrono::Utc::now().format("%a %H:%M").to_string();
    f.edit(|ini| {
        ini.with_section(Some("auto_healing"))
            .set("enabled", "false")
            .set("scrub_schedule_utc", &schedule)
            .set("availability_scan_interval_seconds", "1")
            .set("checksum_scrub_interval_seconds", "1")
            .set("scheduled_scrub_pause_millis", "100")
            .set("object_scan_concurrency", "2")
            .set("shard_repair_concurrency", "1")
            .set("metadata_batch_size", "2")
            .set("topology_check_interval_seconds", "1")
            .set("topology_recovery_stability_seconds", "1");
        ini.with_section(Some("upload_config"))
            .set("monitor_interval_seconds", "1")
            .set("monitor_deep_scan_interval_seconds", "1");
    });
    let server = f.start("127.0.0.1", f.native).await;
    let mut client = f.client("127.0.0.1", f.native, false).await;
    for n in 0..4 {
        put(&mut client, &vec![n; 8193]).await;
    }
    // Stability requires a second topology observation after the configured
    // interval. A fixed 1.2-second sleep races timer and blocking-I/O dispatch.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let health = client
            .control(Command::Health)
            .await
            .unwrap()
            .result
            .unwrap();
        assert!(health["job"].is_null());
        if health["monitor"]["topology_stable"] == true
            && health["monitor"]["last_capacity_check"].is_u64()
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "monitor did not stabilize: {health}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let health = client
        .control(Command::Health)
        .await
        .unwrap()
        .result
        .unwrap();
    assert!(health["job"].is_null());
    assert!(health["monitor"]["last_topology_check"].is_u64());
    assert!(health["monitor"]["last_capacity_check"].is_u64());
    assert_eq!(health["monitor"]["topology_stable"], true);
    drop(client);
    drop(server);
    // The calendar admits a job in its configured UTC minute. Leave enough of
    // that minute for real HDD startup; otherwise a boundary crossing tests the
    // next week's slot instead of this explicitly scheduled scan.
    use chrono::Timelike;
    let second = chrono::Utc::now().second();
    if second > 10 {
        tokio::time::sleep(Duration::from_secs(u64::from(61 - second))).await;
    }
    let schedule = chrono::Utc::now().format("%a %H:%M").to_string();
    f.edit(|ini| {
        ini.with_section(Some("auto_healing"))
            .set("enabled", "true")
            .set("scrub_schedule_utc", schedule);
    });
    let server = f.start("127.0.0.1", f.native).await;
    let mut client = f.client("127.0.0.1", f.native, false).await;
    // Foreground writes continue while the explicitly scheduled scan owns its
    // separate background lane; enabling a schedule does not gate availability.
    put(&mut client, b"foreground-during-scheduled-scrub").await;
    let mut completed = false;
    let mut last_health = Value::Null;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        let health = client
            .control(Command::Health)
            .await
            .unwrap()
            .result
            .unwrap();
        assert_eq!(
            health["configuration"]["scheduled_maintenance"]["calendar_scrub"]["no_scan_timeout"],
            true
        );
        if health["job"]["phase"] == "completed" {
            assert!(health["job"]["report"]["healthy"].as_u64().unwrap() >= 4);
            completed = true;
            break;
        }
        last_health = health;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        completed,
        "configured calendar scrub did not complete: {last_health}; server: {}",
        fs::read_to_string(f.root.join("server.log")).unwrap_or_default()
    );
    drop(client);
    drop(server);
}

#[test]
fn original_no_argument_bootstrap_creates_ini_and_private_env_without_overwrite() {
    let root = tempfile::tempdir().unwrap();
    let run = || {
        Process::new(env!("CARGO_BIN_EXE_litiaina-n1"))
            .current_dir(root.path())
            .output()
            .unwrap()
    };
    assert!(!run().status.success());
    let ini = root.path().join("n1_config.ini");
    let env = root.path().join("n1.env");
    assert!(ini.is_file());
    assert_eq!(fs::metadata(&env).unwrap().permissions().mode() & 0o077, 0);
    let bytes = fs::read(&ini).unwrap();
    assert!(!run().status.success());
    assert_eq!(bytes, fs::read(ini).unwrap());
}

#[tokio::test]
async fn original_systemd_installer_accepts_the_ini_and_preserves_operator_paths() {
    let f = Fixture::new().await;
    let installer =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../scripts/install-n1-systemd.sh");
    let output = Process::new("bash")
        .arg(installer)
        .args(["--install-dir"])
        .arg(&f.root)
        .arg("--binary")
        .arg(env!("CARGO_BIN_EXE_litiaina-n1"))
        .arg("--dry-run")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let unit = String::from_utf8(output.stdout).unwrap();
    assert!(unit.contains("n1_config.ini"));
    assert!(unit.contains("n1.env"));
    assert!(unit.contains("serve --config"));
    assert!(unit.contains("RequiresMountsFor="));
    assert!(!f.root.join("control").exists());
    assert!(!f.root.join("a0/.n1-arc-pool.json").exists());
}

#[tokio::test]
async fn signing_bootstrap_intent_recovers_only_the_exact_saved_key() {
    let source = Fixture::new().await;
    assert!(source.run("init").status.success());
    let key = fs::read(source.root.join("control/link.signing-key")).unwrap();
    let mut intent = source.registry();
    drop(source);
    let f = Fixture::new().await;
    fs::DirBuilder::new()
        .mode(0o700)
        .create(f.root.join("control"))
        .unwrap();
    intent["bootstrap_key"] = serde_json::to_value(&key).unwrap();
    intent["pools"]["A"]["ready"] = false.into();
    intent["pools"]["A"]["config"]["roots"] = serde_json::json!([f.root.join("a0")]);
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(f.registry_file())
        .unwrap();
    file.write_all(&serde_json::to_vec(&intent).unwrap())
        .unwrap();
    file.sync_all().unwrap();
    drop(file);
    let run = f.run("init");
    assert!(
        run.status.success(),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );
    assert_eq!(
        key,
        fs::read(f.root.join("control/link.signing-key")).unwrap()
    );
    assert!(f.registry()["bootstrap_key"].is_null());
    assert_eq!(
        intent["pools"]["A"]["config"]["layout"],
        f.registry()["pools"]["A"]["config"]["layout"]
    );
    assert!(f.run("check").status.success());
}

#[tokio::test]
#[ignore = "physical INI witness: explicit N1_INI_TEST_HDD_ROOT and N1_INI_TEST_SECOND_HDD required"]
async fn physical_ini_pool_expansion_and_scheduled_staging_gc_preserve_old_keys() {
    use std::os::unix::fs::MetadataExt;
    let f = Fixture::new().await;
    let parent =
        PathBuf::from(std::env::var_os("N1_INI_TEST_SECOND_HDD").expect("second HDD required"));
    let second = parent.join(format!("n1-ini-{}", ulid::Ulid::new()));
    fs::DirBuilder::new().mode(0o700).create(&second).unwrap();
    assert_ne!(
        fs::metadata(&second).unwrap().dev(),
        fs::metadata(&f.root).unwrap().dev()
    );
    fs::create_dir(second.join("a1")).unwrap();
    f.edit(|ini| {
        ini.with_section(Some("storage_pool.A"))
            .set(
                "roots",
                format!(
                    "{},{}",
                    f.root.join("a0").display(),
                    second.join("a1").display()
                ),
            )
            .set("parity_shards", "1");
        ini.with_section(Some("garbage_collector"))
            .set("cleanup_interval_seconds", "1")
            .set("file_ttl_seconds", "1");
    });
    let server = f.start("127.0.0.1", f.native).await;
    let mut client = f.client("127.0.0.1", f.native, false).await;
    let bytes = (0..131_195).map(|n| (n % 251) as u8).collect::<Vec<_>>();
    let first = put(&mut client, &bytes).await;
    let registry = f.registry();
    let marker0 = fs::read(f.root.join("a0/.n1-arc-pool.json")).unwrap();
    let marker1 = fs::read(second.join("a1/.n1-arc-pool.json")).unwrap();
    drop(client);
    drop(server);
    fs::create_dir(f.root.join("b0")).unwrap();
    fs::create_dir(second.join("b1")).unwrap();
    f.edit(|ini| {
        ini.with_section(Some("storage_pool.B"))
            .set(
                "roots",
                format!(
                    "{},{}",
                    f.root.join("b0").display(),
                    second.join("b1").display()
                ),
            )
            .set("data_shards", "1")
            .set("parity_shards", "1")
            .set("generation", "1")
            .set("sealed", "false");
    });
    let server = f.start("127.0.0.1", f.native).await;
    let mut client = f.client("127.0.0.1", f.native, false).await;
    let second_object = put(&mut client, &bytes).await;
    let updated = f.registry();
    let key: [u8; 32] = fs::read(f.root.join("control/link.signing-key"))
        .unwrap()
        .try_into()
        .unwrap();
    let signer = n1_link::KeySigner::new(key);
    let scope: n1_link::PoolId = serde_json::from_value(updated["principal"].clone()).unwrap();
    let old = signer
        .verify_object(&scope.0, first["object_key"].as_str().unwrap())
        .unwrap();
    let new = signer
        .verify_object(&scope.0, second_object["object_key"].as_str().unwrap())
        .unwrap();
    assert_eq!(
        old.pool.to_string(),
        registry["pools"]["A"]["config"]["layout"]["id"]
    );
    assert_eq!(
        new.pool.to_string(),
        updated["pools"]["B"]["config"]["layout"]["id"]
    );
    assert_ne!(old.pool, new.pool);
    let pending = client
        .control(Command::UploadInit {
            pool: None,
            total_bytes: 4096,
            part_bytes: 4096,
            durable: true,
            ttl_seconds: 1,
        })
        .await
        .unwrap()
        .result
        .unwrap();
    client
        .write_at(
            pending["object_key"].as_str().unwrap().into(),
            0,
            4096,
            &mut vec![4u8; 4096].as_slice(),
        )
        .await
        .unwrap();
    let mut reclaimed = false;
    for _ in 0..80 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Err(n1_native_client::NativeClientError::Native { code, .. }) = client
            .control(Command::UploadStatus {
                object_key: pending["object_key"].as_str().unwrap().into(),
                start: 0,
                limit: 1,
            })
            .await
            && code == "404"
        {
            reclaimed = true;
            break;
        }
    }
    assert!(
        reclaimed,
        "configured scheduled GC did not reclaim expired staged bytes"
    );
    let mut read = Vec::new();
    client
        .download(
            first["object_key"].as_str().unwrap().into(),
            0,
            None,
            &mut read,
        )
        .await
        .unwrap();
    assert_eq!(read, bytes);
    assert_eq!(
        marker0,
        fs::read(f.root.join("a0/.n1-arc-pool.json")).unwrap()
    );
    assert_eq!(
        marker1,
        fs::read(second.join("a1/.n1-arc-pool.json")).unwrap()
    );
    drop(client);
    drop(server);
    fs::write(
        f.root.join("PASSED.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "passed": true,
            "scope": "INI enrollment/restart/two-HDD expansion and scheduled staging GC",
            "primary": f.root,
            "secondary": second,
            "pool_a": old.pool,
            "pool_b": new.pool,
            "verified_bytes": bytes.len(),
            "scheduled_gc_reclaimed": true
        }))
        .unwrap(),
    )
    .unwrap();
    let kept = f._root.keep();
    println!("physical INI evidence: {}", kept.display());
}
