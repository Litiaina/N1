//! Opt-in real Console -> Shell operator -> LINK backup/replication/restore.
//! RAM fixtures qualify the control boundary; physical HDD qualification is
//! separate. Rust owns services, payloads, recovery identities and the oracle.
#![cfg(target_os = "linux")]
use cli_core::{Credentials, result};
use n1_link::noa::Command as Native;
use serde_json::{Value, json};
use std::{
    fs,
    io::Write,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
fn private(path: &Path, value: &Value) {
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .unwrap();
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .unwrap();
    file.write_all(&serde_json::to_vec_pretty(value).unwrap())
        .unwrap();
}
fn command(binary: &Path, args: &[&str]) {
    let output = Command::new(binary).args(args).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
fn start(binary: &Path, args: &[&str], root: &Path, name: &str) -> Process {
    let log = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(root.join(format!("{name}.log")))
        .unwrap();
    Process(
        Command::new(binary)
            .args(args)
            .current_dir(root)
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .stdin(Stdio::null())
            .spawn()
            .unwrap(),
    )
}
async fn ready(port: u16, child: &mut Process) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            assert!(child.0.try_wait().unwrap().is_none(), "fixture exited");
            if tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
}
fn configured(
    binary: &Path,
    root: &Path,
    label: &str,
    port: u16,
    repository: &Path,
    key: &Path,
) -> PathBuf {
    let config = root.join(format!("{label}.json"));
    let member = root.join(format!("{label}-member"));
    fs::create_dir(&member).unwrap();
    command(
        binary,
        &[
            "init",
            "--config",
            config.to_str().unwrap(),
            "--member",
            member.to_str().unwrap(),
            "--data-shards",
            "1",
            "--parity-shards",
            "0",
            "--block-bytes",
            "65536",
            "--native-address",
            &format!("127.0.0.1:{port}"),
        ],
    );
    let mut value: Value = serde_json::from_slice(&fs::read(&config).unwrap()).unwrap();
    value["repositories"] = json!({"primary":{"path":repository,"key_file":key}});
    fs::write(&config, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    config
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires private fixture root, real object binary, Console/Node/Chromium"]
async fn console_operator_captures_replicates_restores_and_preserves_retirement() {
    let parent = PathBuf::from(
        std::env::var_os("N1_OPERATOR_TEST_ROOT").expect("explicit proof parent required"),
    );
    assert_eq!(
        fs::metadata(&parent).unwrap().permissions().mode() & 0o077,
        0,
        "private proof parent required"
    );
    let root = parent.join(format!("console-operator-{}", ulid::Ulid::new()));
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    eprintln!("Console object recovery evidence: {}", root.display());
    let binary = PathBuf::from(
        std::env::var_os("N1_OBJECT_TEST_BINARY").expect("frozen object binary required"),
    );
    let shell = std::env::var_os("N1_OPERATOR_TEST_SHELL_BINARY")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_litiaina-n1-shell")));
    let key = root.join("backup.key");
    command(
        &binary,
        &["backup", "keygen", "--output", key.to_str().unwrap()],
    );
    let repo = root.join("primary");
    let replica = root.join("replica");
    // Both native repositories use the same encryption identity for byte-copy
    // replication. Different-key rejection has separate physical coverage.
    command(
        &binary,
        &[
            "backup",
            "init",
            "--repository",
            replica.to_str().unwrap(),
            "--key-file",
            key.to_str().unwrap(),
        ],
    );
    let source_port = port();
    let receiver_port = port();
    let operator_port = port();
    let console_port = port();
    let source = configured(&binary, &root, "source", source_port, &repo, &key);
    let receiver = configured(&binary, &root, "receiver", receiver_port, &replica, &key);
    let mut source_process = start(
        &binary,
        &["serve", "--config", source.to_str().unwrap()],
        &root,
        "source",
    );
    ready(source_port, &mut source_process).await;
    let mut receiver_process = start(
        &binary,
        &["serve", "--config", receiver.to_str().unwrap()],
        &root,
        "receiver",
    );
    ready(receiver_port, &mut receiver_process).await;
    let credentials = Credentials::load(&source.with_extension("credentials.json")).unwrap();
    let mut client = credentials.connect(None, None).await.unwrap();
    let bytes = (0..8 * 1024 * 1024 + 19)
        .map(|n| (n % 251) as u8)
        .collect::<Vec<_>>();
    let mut keys = Vec::new();
    for payload in [&bytes[..], &[31u8; 4096][..]] {
        let init = result(
            client
                .control(Native::UploadInit {
                    pool: None,
                    total_bytes: payload.len() as u64,
                    part_bytes: (payload.len() as u64).div_ceil(65536) * 65536,
                    durable: true,
                    ttl_seconds: 3600,
                })
                .await
                .unwrap(),
        )
        .unwrap();
        client
            .put(
                init["object_key"].as_str().unwrap().into(),
                payload.len() as u64,
                &mut std::io::Cursor::new(payload.to_vec()),
            )
            .await
            .unwrap();
        keys.push(init);
    }
    let retired = keys[1]["object_key"].as_str().unwrap().to_owned();
    client
        .control(Native::ObjectDelete {
            object_key: retired.clone(),
        })
        .await
        .unwrap();
    drop(client);
    let target = root.join("restored-member");
    fs::create_dir(&target).unwrap();
    let restore_ini = root.join("restore.ini");
    cli_core::settings::create_default_config_ini(&restore_ini).unwrap();
    // The operator resolves preconfigured target roots; no browser path is
    // accepted. A minimal valid 1+0 target is enough for this control proof.
    let mut settings = ini::Ini::load_from_file(&restore_ini).unwrap();
    settings
        .with_section(Some("storage_pool.A"))
        .set("roots", target.to_str().unwrap())
        .set("data_shards", "1")
        .set("parity_shards", "0");
    settings.with_section(Some("disk_config")).set(
        "fs_storage_path",
        root.join("target-control").to_str().unwrap(),
    );
    settings.write_to_file(&restore_ini).unwrap();
    let token_file = root.join("operator.token");
    let output = Command::new(&shell)
        .args(["backup", "operator-keygen", "--token-file"])
        .arg(&token_file)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let operator = root.join("operator.json");
    let certificate = root.join("operator-cert.pem");
    let private_key = root.join("operator-tls.key");
    let generated = Command::new("openssl")
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
            "subjectAltName=IP:127.0.0.1",
            "-keyout",
        ])
        .arg(&private_key)
        .arg("-out")
        .arg(&certificate)
        .output()
        .unwrap();
    assert!(generated.status.success());
    private(
        &operator,
        &json!({
            "bind": format!("127.0.0.1:{operator_port}"),
            "token_file": token_file,
            "tls_cert": certificate,
            "tls_key": private_key,
            "allowed_origins": [format!("http://127.0.0.1:{console_port}")],
            "repositories": {
                "primary": {
                    "path": repo,
                    "key_file": key
                }
            },
            "engine_config": source,
            "engine_binary": binary,
            "online_source": {
                "credentials_file": source.with_extension("credentials.json"),
                "repository": "primary"
            },
            "replicas": {
                "remote": {
                    "credentials_file": receiver.with_extension("credentials.json"),
                    "repository": "primary"
                }
            },
            "restore_targets": {
                "empty": restore_ini
            },
            "capture_boundary_seconds": 30
        }),
    );
    let mut operator_process = start(
        &shell,
        &["backup", "serve", "--config", operator.to_str().unwrap()],
        &root,
        "operator",
    );
    ready(operator_port, &mut operator_process).await;
    let fixture = root.join("browser.json");
    private(
        &fixture,
        &json!({
            "operator": format!("https://127.0.0.1:{operator_port}"),
            "certificate": certificate,
            "token_file": token_file,
            "console_port": console_port,
            "root": root
        }),
    );
    let console =
        PathBuf::from(std::env::var_os("N1_CONSOLE_REPO").expect("Console repo required"));
    let browser_log = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(root.join("browser.log"))
        .unwrap();
    let mut browser =
        tokio::process::Command::new(std::env::var_os("N1_BROWSER_NODE").expect("Node required"))
            .arg(console.join("tests/n1-link-backups.live.mjs"))
            .arg("--fixture")
            .arg(&fixture)
            .current_dir(&console)
            .stdin(Stdio::null())
            .stdout(browser_log.try_clone().unwrap())
            .stderr(browser_log)
            .kill_on_drop(true)
            .spawn()
            .unwrap();
    let mut source_stopped = false;
    let status = tokio::time::timeout(Duration::from_secs(180), async {
        loop {
            if let Some(status) = browser.try_wait().unwrap() {
                break status;
            }
            if !source_stopped && root.join("stop-source-requested").exists() {
                // Rust retains ownership of the real process and graceful
                // shutdown; JavaScript only requests the next browser phase.
                assert_eq!(
                    unsafe { libc::kill(source_process.0.id() as i32, libc::SIGTERM) },
                    0
                );
                tokio::time::timeout(Duration::from_secs(30), async {
                    loop {
                        if let Some(status) = source_process.0.try_wait().unwrap() {
                            assert!(status.success());
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                })
                .await
                .unwrap();
                source_stopped = true;
                private(&root.join("source-stopped"), &json!({"graceful":true}));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert!(status.success(), "inspect {}/browser.log", root.display());
    let report: Value =
        serde_json::from_slice(&fs::read(root.join("browser-PASSED.json")).unwrap()).unwrap();
    assert_eq!(report["passed"], true);
    assert!(
        source_stopped,
        "offline capture must follow Rust-owned graceful shutdown"
    );
    let snapshot = report["snapshot"].as_str().unwrap();
    command(
        &binary,
        &[
            "backup",
            "verify",
            "--repository",
            replica.to_str().unwrap(),
            "--key-file",
            key.to_str().unwrap(),
            "--snapshot",
            snapshot,
        ],
    );
    let mut restored: Value = serde_json::from_slice(&fs::read(&source).unwrap()).unwrap();
    let restored_port = port();
    restored["pools"][0]["roots"] = json!([target]);
    restored["native_address"] = json!(format!("127.0.0.1:{restored_port}"));
    restored["state_dir"] = json!(root.join("restored-control"));
    restored["repositories"] = json!({});
    let restored_config = root.join("restored.json");
    private(&restored_config, &restored);
    let mut restored_process = start(
        &binary,
        &["serve", "--config", restored_config.to_str().unwrap()],
        &root,
        "restored",
    );
    ready(restored_port, &mut restored_process).await;
    let mut client = credentials
        .connect(Some(&format!("n1://127.0.0.1:{restored_port}")), None)
        .await
        .unwrap();
    let mut output = Vec::new();
    client
        .download(
            keys[0]["object_key"].as_str().unwrap().into(),
            0,
            None,
            &mut output,
        )
        .await
        .unwrap();
    assert_eq!(output, bytes);
    assert!(
        client
            .control(Native::ObjectStat {
                object_key: retired
            })
            .await
            .is_err()
    );
    assert!(
        client
            .put(
                keys[1]["object_key"].as_str().unwrap().into(),
                4096,
                &mut std::io::Cursor::new(vec![31u8; 4096])
            )
            .await
            .is_err(),
        "a purged LINK identity must stay absent after recovery"
    );
    private(
        &root.join("PASSED.json"),
        &json!({
            "passed": true,
            "scope": "real Console, HTTPS operator, native replication and 1+0 restore",
            "snapshot": snapshot,
            "payload_bytes_verified": bytes.len(),
            "permanent_retirement_verified": true,
            "replica_verified": true
        }),
    );
}
