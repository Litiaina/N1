//! Real public metadata -> native object API qualification. No mocked engine.
use m1::server::native::types::{NativeRequest, NativeResponse};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[path = "noa_service/accel.rs"]
mod accel;
#[path = "noa_service/admission.rs"]
mod admission;
#[path = "noa_service/archives.rs"]
mod archives;
#[path = "noa_service/bridge_status.rs"]
mod bridge_status;
#[path = "noa_service/console_workflows.rs"]
mod console_workflows;
#[path = "noa_service/crash_matrix.rs"]
mod crash_matrix;
#[path = "noa_service/device_cycle.rs"]
mod device_cycle;
#[path = "noa_service/diagnostic_load.rs"]
mod diagnostic_load;
#[path = "noa_service/export_lifecycle.rs"]
mod export_lifecycle;
#[path = "noa_service/export_performance.rs"]
mod export_performance;
#[path = "noa_service/gc_concurrent.rs"]
mod gc_concurrent;
#[path = "noa_service/lost_allocation.rs"]
mod lost_allocation;
#[path = "noa_service/maintenance.rs"]
mod maintenance;
#[path = "noa_service/maintenance_load.rs"]
mod maintenance_load;
#[path = "noa_service/monitoring.rs"]
mod monitoring;
#[path = "noa_service/monitoring_performance.rs"]
mod monitoring_performance;
#[path = "noa_service/multipart_bridge.rs"]
mod multipart_bridge;
#[path = "noa_service/multipart_geometry.rs"]
mod multipart_geometry;
#[path = "noa_service/object_recovery.rs"]
mod object_recovery;
#[path = "noa_service/object_status.rs"]
mod object_status;
#[path = "noa_service/preparation_bench.rs"]
mod preparation_bench;
#[path = "noa_service/reliability.rs"]
mod reliability;
#[path = "noa_service/resource_exhaustion.rs"]
mod resource_exhaustion;
#[path = "noa_service/resume_damage.rs"]
mod resume_damage;
#[path = "noa_service/resume_listing.rs"]
mod resume_listing;
#[path = "noa_service/resume_progress.rs"]
mod resume_progress;
#[path = "noa_service/resume_scale.rs"]
mod resume_scale;
#[path = "noa_service/search.rs"]
mod search;
#[path = "noa_service/single_part.rs"]
mod single_part;
#[path = "noa_service/staged_recovery.rs"]
mod staged_recovery;
#[path = "noa_service/stalled_io.rs"]
mod stalled_io;
#[path = "noa_service/upload_regression.rs"]
mod upload_regression;
#[path = "noa_service/upload_size_limits.rs"]
mod upload_size_limits;
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
struct Stack {
    root: PathBuf,
    object: Option<Process>,
    metadata: Option<Process>,
    url: String,
    native: u16,
    object_binary: PathBuf,
    object_config: PathBuf,
}
fn port() -> u16 {
    // Closing a probe lets the OS immediately return the same port again.
    // Keep allocations distinct within this fixture process so HTTP/native
    // listeners cannot receive identical configuration before either starts.
    static ALLOCATED: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<u16>>> =
        std::sync::OnceLock::new();
    let mut allocated = ALLOCATED.get_or_init(Default::default).lock().unwrap();
    loop {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        if allocated.insert(port) {
            return port;
        }
    }
}
fn spawn(binary: &Path, args: &[&str], root: &Path, log: &str) -> Process {
    // This fixture verifies the informational N1 startup banner explicitly.
    spawn_with_env(binary, args, root, log, &[("RUST_LOG", "info")])
}
fn spawn_with_env(
    binary: &Path,
    args: &[&str],
    root: &Path,
    log: &str,
    environment: &[(&str, &str)],
) -> Process {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join(log))
        .unwrap();
    Process(
        Command::new(binary)
            .args(args)
            .envs(environment.iter().copied())
            .current_dir(root)
            .stdout(Stdio::from(file.try_clone().unwrap()))
            .stderr(Stdio::from(file))
            .spawn()
            .unwrap(),
    )
}
impl Stack {
    async fn new() -> Self {
        Self::with_block_bytes(4096).await
    }
    async fn with_block_bytes(block_bytes: u32) -> Self {
        let parent = PathBuf::from(
            std::env::var_os("N1_OBJECT_TEST_ROOT").expect("explicit HDD parent required"),
        );
        let root = parent.join(format!("noa-service-{}", ulid::Ulid::new()));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&root)
            .unwrap();
        eprintln!("public NOA evidence: {}", root.display());
        let (members, data, parity) =
            if let Some(paths) = std::env::var_os("N1_OBJECT_TEST_FOUR_ROOTS") {
                use std::{collections::HashSet, os::unix::fs::MetadataExt};
                assert!(std::env::var_os("N1_OBJECT_TEST_FIRST_ROOT").is_none());
                assert!(std::env::var_os("N1_OBJECT_TEST_SECOND_ROOT").is_none());
                let parents = std::env::split_paths(&paths)
                    .map(|path| std::fs::canonicalize(path).unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(parents.len(), 4, "four explicit member parents required");
                let devices = parents
                    .iter()
                    .map(|p| p.metadata().unwrap().dev())
                    .collect::<HashSet<_>>();
                assert_eq!(devices.len(), 4, "four independent filesystems required");
                let data = std::env::var("N1_OBJECT_TEST_DATA_SHARDS")
                    .expect("explicit four-member data shard count required")
                    .parse::<u16>()
                    .unwrap();
                assert!(
                    [2, 3].contains(&data),
                    "only 3+1 and 2+2 fixtures supported"
                );
                (
                    parents
                        .iter()
                        .map(|p| p.join(root.file_name().unwrap()))
                        .collect(),
                    data,
                    4 - data,
                )
            } else {
                assert!(
                    std::env::var_os("N1_OBJECT_TEST_DATA_SHARDS").is_none(),
                    "four-member geometry requires four explicit member parents"
                );
                let first = std::env::var_os("N1_OBJECT_TEST_FIRST_ROOT")
                    .map(|parent| PathBuf::from(parent).join(root.file_name().unwrap()))
                    .unwrap_or_else(|| root.join("member0"));
                let second = std::env::var_os("N1_OBJECT_TEST_SECOND_ROOT")
                    .map(|p| PathBuf::from(p).join(root.file_name().unwrap()));
                let parity = u16::from(second.is_some());
                (
                    std::iter::once(first).chain(second).collect::<Vec<_>>(),
                    1,
                    parity,
                )
            };
        for path in &members {
            std::fs::create_dir(path).unwrap();
        }
        let object_binary = PathBuf::from(
            std::env::var_os("N1_OBJECT_TEST_BINARY").expect("object binary required"),
        );
        let object_config = root.join("object.json");
        let object_port = port();
        let mut init = Command::new(&object_binary);
        init.args(["init", "--config"]).arg(&object_config);
        for member in &members {
            init.arg("--member").arg(member);
        }
        let output = init
            .args([
                "--data-shards",
                &data.to_string(),
                "--parity-shards",
                &parity.to_string(),
                "--block-bytes",
                &block_bytes.to_string(),
                "--native-address",
                &format!("127.0.0.1:{object_port}"),
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let credentials: Value = serde_json::from_slice(
            &std::fs::read(object_config.with_extension("credentials.json")).unwrap(),
        )
        .unwrap();
        let mut object_settings: Value =
            serde_json::from_slice(&std::fs::read(&object_config).unwrap()).unwrap();
        object_settings["runtime"]["monitor_interval_seconds"] = json!(1);
        // Functional/recovery witnesses must not turn a fast archive or scan
        // into a test of N1's default 30-reads/s quota. M1 quota cases below
        // explicitly enable their policy; N1's own quota tests are separate.
        object_settings["rate_limits"]["enabled"] = json!(false);
        std::fs::write(
            &object_config,
            serde_json::to_vec_pretty(&object_settings).unwrap(),
        )
        .unwrap();
        let http = port();
        let native = port();
        let ini_path = root.join("m1_config.ini");
        cli_core::settings::create_default_config_ini(ini_path.to_str().unwrap()).unwrap();
        let mut ini = ini::Ini::load_from_file(&ini_path).unwrap();
        ini.with_section(Some("server"))
            .set("host", "127.0.0.1")
            .set("https_port", http.to_string())
            .set("native_port", native.to_string())
            .set("cert_path", "")
            .set("key_path", "");
        ini.with_section(Some("disk_config")).set(
            "fs_storage_path",
            root.join("metadata-control").display().to_string(),
        );
        ini.with_section(Some("storage_pool.A"))
            .set("generation", "1")
            .set(
                "roots",
                members
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            )
            .set("data_shards", data.to_string())
            .set("parity_shards", parity.to_string())
            .set("sealed", "false");
        ini.with_section(Some("upload_config"))
            .set("max_concurrent_io", "32")
            .set("max_part_size", "52428800")
            .set("upload_memory_budget_bytes", "67108864");
        ini.with_section(Some("upload_config"))
            .set("monitor_interval_seconds", "1");
        // Functional/fault witnesses are not request-rate campaigns. Explicit
        // quota tests below re-enable and exercise the operator policy.
        ini.with_section(Some("rate_limiter_config"))
            .set("enable_rate_limiting", "false");
        ini.with_section(Some("metadata_engine"))
            .set(
                "object_native_url",
                credentials["native_url"].as_str().unwrap(),
            )
            .set("object_fragment", credentials["fragment"].as_str().unwrap())
            .set("object_connections", "16");
        ini.with_section(Some("metadata_engine")).set(
            "backup_dir",
            root.join("metadata-backups").display().to_string(),
        );
        ini.write_to_file(&ini_path).unwrap();
        let env = root.join("m1.env");
        std::fs::write(&env,format!("JWT_SECRET=test-only-private-metadata-jwt-{}\nAUTH_KEY={}\nMASTER_KEY=test-only-master\nADMIN_KEY=test-only-admin\n",ulid::Ulid::new(),credentials["secret"].as_str().unwrap())).unwrap();
        std::fs::set_permissions(env, std::fs::Permissions::from_mode(0o600)).unwrap();
        let mut stack = Self {
            root,
            object: None,
            metadata: None,
            url: format!("http://127.0.0.1:{http}"),
            native,
            object_binary,
            object_config,
        };
        stack.start_object().await;
        stack.start_metadata().await;
        stack
    }
    async fn start_object(&mut self) {
        self.object = Some(spawn(
            &self.object_binary,
            &["serve", "--config", self.object_config.to_str().unwrap()],
            &self.root,
            "object.log",
        ));
        let credentials: Value = serde_json::from_slice(
            &std::fs::read(self.object_config.with_extension("credentials.json")).unwrap(),
        )
        .unwrap();
        let url = credentials["native_url"]
            .as_str()
            .unwrap()
            .trim_start_matches("n1://");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
        while tokio::time::Instant::now() < deadline {
            if tokio::net::TcpStream::connect(url).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!(
            "object native listener did not become ready within 180 seconds: {}",
            std::fs::read_to_string(self.root.join("object.log")).unwrap()
        );
    }
    async fn start_metadata(&mut self) {
        self.start_metadata_with_env(&[]).await;
    }
    async fn start_metadata_with_env(&mut self, environment: &[(&str, &str)]) {
        let binary = std::env::var_os("M1_TEST_BINARY")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_litiaina-m1")));
        self.start_metadata_binary(&binary, environment).await;
    }
    async fn start_metadata_binary(&mut self, binary: &Path, environment: &[(&str, &str)]) {
        self.metadata = Some(spawn_with_env(
            binary,
            &["serve"],
            &self.root,
            "metadata.log",
            environment,
        ));
        let client = Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
        while tokio::time::Instant::now() < deadline {
            if client
                .get(format!("{}/noa/v1/ping", self.url))
                .send()
                .await
                .is_ok_and(|r| r.status() == StatusCode::OK)
            {
                return;
            }
            if self
                .metadata
                .as_mut()
                .unwrap()
                .0
                .try_wait()
                .unwrap()
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!(
            "metadata HTTP listener did not become ready within 180 seconds: {}",
            std::fs::read_to_string(self.root.join("metadata.log")).unwrap()
        );
    }
}
async fn json_response(response: reqwest::Response, status: u16) -> Value {
    let actual = response.status().as_u16();
    let route = response.url().path().to_owned();
    let bytes = response.bytes().await.unwrap();
    assert_eq!(
        actual,
        status,
        "{route}: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).unwrap()
}
async fn native(
    socket: &mut tokio::net::TcpStream,
    op: &str,
    payload: Value,
    body: Option<&[u8]>,
) -> (NativeResponse, Vec<u8>) {
    let request = NativeRequest {
        id: ulid::Ulid::new().to_string(),
        op: op.into(),
        payload: Some(payload.to_string()),
        body: None,
        body_len: body.map(|b| b.len() as u64),
    };
    let control = rmp_serde::to_vec_named(&request).unwrap();
    socket.write_u32(control.len() as u32).await.unwrap();
    socket.write_all(&control).await.unwrap();
    if let Some(body) = body {
        socket.write_all(body).await.unwrap();
    }
    socket.flush().await.unwrap();
    let length = socket.read_u32().await.unwrap();
    assert!(length <= 8 * 1024 * 1024);
    let mut control = vec![0; length as usize];
    socket.read_exact(&mut control).await.unwrap();
    let response: NativeResponse = rmp_serde::from_slice(&control).unwrap();
    let mut bytes = vec![0; response.body_len.unwrap_or(0) as usize];
    socket.read_exact(&mut bytes).await.unwrap();
    (response, bytes)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real public API witness requires an explicit object binary and HDD roots"]
async fn public_noa_metadata_object_restart_and_native_contract() {
    let mut stack = Stack::new().await;
    let metadata_banner = std::fs::read_to_string(stack.root.join("metadata.log")).unwrap();
    let object_banner = std::fs::read_to_string(stack.root.join("object.log")).unwrap();
    assert!(metadata_banner.contains("L I T I A I N A  M 1"));
    assert!(metadata_banner.contains("M1 metadata store"));
    assert!(metadata_banner.contains("Database       : M1 native storage"));
    assert!(metadata_banner.contains("Start Time     :"));
    assert!(!metadata_banner.contains("L I T I A I N A  N 1"));
    assert!(object_banner.contains("L I T I A I N A  N 1"));
    assert!(object_banner.contains("N1 object store"));
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let ping = json_response(
        client.get(format!("{base}/ping")).send().await.unwrap(),
        200,
    )
    .await;
    assert_eq!(ping["engine"], "m1");
    assert_eq!(ping["version"], env!("CARGO_PKG_VERSION"));
    let created=json_response(client.post(format!("{base}/fragments")).json(&json!({"name":"NOA witness","secret":"test-only-master","key":"test-only-fragment"})).send().await.unwrap(),201).await;
    let fragment = created["fragment_hash"].as_str().unwrap();
    let logged = json_response(
        client
            .post(format!("{base}/auth/fragment"))
            .json(&json!({
                "fragment": fragment,
                "secret": "test-only-fragment",
                "read": true,
                "write": true,
                "modify": true,
                "delete": true
            }))
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let token = logged["access_token"].as_str().unwrap().to_owned();
    let bytes = (0..131195).map(|n| (n % 251) as u8).collect::<Vec<_>>();
    let path = "a/hello.bin";
    let first = json_response(
        client
            .post(format!("{base}/upload/one-shot/{path}"))
            .bearer_auth(&token)
            .header("content-type", "application/octet-stream")
            .body(bytes.clone())
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
    let response = client
        .get(format!("{base}/objects/get/{path}"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
    let range = client
        .get(format!("{base}/objects/get/{path}"))
        .bearer_auth(&token)
        .header("range", "bytes=100-399")
        .send()
        .await
        .unwrap();
    assert_eq!(range.status(), 206);
    assert_eq!(range.bytes().await.unwrap().as_ref(), &bytes[100..400]);
    let _ = json_response(
        client
            .post(format!("{base}/upload/one-shot/{path}"))
            .bearer_auth(&token)
            .body("new immutable version")
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
    let pinned = client
        .get(format!("{base}/objects/get/{path}"))
        .query(&[("version_id", first["version_id"].as_str().unwrap())])
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(pinned.bytes().await.unwrap().as_ref(), bytes);
    let versions = json_response(
        client
            .get(format!("{base}/objects/versions/{path}"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(versions.as_array().unwrap().len(), 2);
    let listing = json_response(
        client
            .get(format!("{base}/posix/list"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert!(
        listing["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["path"] == "a" && v["kind"] == "directory")
    );
    // Same upload shape reuses its identity; external parts intentionally do not
    // align to the pool's immutable stripe width.
    let shape = json!({
        "content_type": "application/octet-stream",
        "total_size_bytes": bytes.len(),
        "expected_parts": 3,
        "part_size_bytes": 50001
    });
    let init_url = format!("{base}/upload/multipart/durable/init/resume.bin");
    let upload = json_response(
        client
            .post(&init_url)
            .bearer_auth(&token)
            .json(&shape)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let retry = json_response(
        client
            .post(&init_url)
            .bearer_auth(&token)
            .json(&shape)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(upload, retry);
    let version = upload["version_id"].as_str().unwrap();
    let session = upload["session_id"].as_str().unwrap();
    for index in [2, 0] {
        let start = index * 50001;
        let end = bytes.len().min(start + 50001);
        let _ = json_response(
            client
                .put(format!("{base}/upload/multipart/durable/{version}/{index}"))
                .bearer_auth(&token)
                .body(bytes[start..end].to_vec())
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
    }
    let status = json_response(
        client
            .get(format!("{base}/upload/multipart/durable/{version}"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(status["uploaded_parts"], json!([0, 2]));
    drop(stack.metadata.take());
    drop(stack.object.take());
    stack.start_object().await;
    stack.start_metadata().await;
    let status = json_response(
        client
            .get(format!("{base}/upload/multipart/durable/{version}"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(status["uploaded_parts"], json!([0, 2]));
    let _ = json_response(
        client
            .put(format!("{base}/upload/multipart/durable/{version}/1"))
            .bearer_auth(&token)
            .body(bytes[50001..100002].to_vec())
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let finalize = format!("{base}/upload/multipart/durable/{version}/{session}/finalize");
    let finished = json_response(
        client
            .post(&finalize)
            .bearer_auth(&token)
            .json(&shape)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let repeated = json_response(
        client
            .post(&finalize)
            .bearer_auth(&token)
            .json(&shape)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(finished, repeated);
    let read = client
        .get(format!("{base}/objects/get/resume.bin"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(read.bytes().await.unwrap().as_ref(), bytes);
    // The original native wire and operation names remain usable.
    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", stack.native))
        .await
        .unwrap();
    let (ping, _) = native(&mut socket, "ping", json!({}), None).await;
    assert!(ping.ok);
    let ping: Value = serde_json::from_str(ping.payload.as_deref().unwrap()).unwrap();
    assert_eq!(ping["server"], "Litiaina M1");
    assert_eq!(ping["protocol"], "n1/0.5");
    let (authenticated, _) = native(&mut socket, "auth.bearer", json!({"token":token}), None).await;
    assert!(authenticated.ok, "{:?}", authenticated.error);
    let (put, body) = native(
        &mut socket,
        "upload.one_shot",
        json!({"object_key":"native.txt","content_type":"text/plain"}),
        Some(b"native metadata to LINK"),
    )
    .await;
    assert!(put.ok, "{}", String::from_utf8_lossy(&body));
    assert_eq!(put.status, Some(201));
    let (get, body) = native(
        &mut socket,
        "object.get",
        json!({"object_key":"native.txt"}),
        None,
    )
    .await;
    assert!(get.ok);
    assert_eq!(body, b"native metadata to LINK");
    drop(socket);
    let moved = json_response(
        client
            .post(format!("{base}/posix/rename"))
            .bearer_auth(&token)
            .json(&json!({"source_path":"a/","destination_path":"moved/"}))
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(moved["affected"], 1);
    let trash = json_response(
        client
            .delete(format!("{base}/objects/moved/hello.bin"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(
        client
            .get(format!("{base}/objects/get/moved/hello.bin"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    let _ = json_response(
        client
            .post(format!("{base}/posix/recover"))
            .bearer_auth(&token)
            .json(&json!({"path":"moved/hello.bin","trash_entry_id":trash["trash_entry_id"]}))
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let form = reqwest::multipart::Form::new()
        .part(
            "object",
            reqwest::multipart::Part::bytes(b"batch bytes".to_vec()).file_name("batch%2Fone.txt"),
        )
        .part(
            "object",
            reqwest::multipart::Part::bytes(Vec::new()).file_name("empty.txt"),
        );
    let results = json_response(
        client
            .post(format!("{base}/upload/batch"))
            .bearer_auth(&token)
            .multipart(form)
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
    assert_eq!(results["uploaded"], 2);
    let read_only = json_response(
        client
            .post(format!("{base}/auth/fragment"))
            .json(&json!({"fragment":fragment,"secret":"test-only-fragment","read":true}))
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(
        client
            .post(format!("{base}/upload/one-shot/forbidden"))
            .bearer_auth(read_only["access_token"].as_str().unwrap())
            .body("forbidden")
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    let signed = json_response(
        client
            .post(format!("{base}/objects/signed-url"))
            .bearer_auth(&token)
            .json(&json!({"object_key":"resume.bin"}))
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let read = client
        .get(signed["url"].as_str().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(read.status(), 200);
    assert_eq!(read.bytes().await.unwrap().as_ref(), bytes);
    let refreshed = json_response(
        client
            .post(format!("{base}/auth/refresh"))
            .json(&json!({"refresh_token":logged["refresh_token"]}))
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(
        client
            .post(format!("{base}/auth/refresh"))
            .json(&json!({"refresh_token":logged["refresh_token"]}))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        client
            .post(format!("{base}/auth/refresh"))
            .json(&json!({"refresh_token":refreshed["refresh_token"]}))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    // The original single-part API permits omitted content type/part size,
    // including an empty object represented by one empty external part.
    let empty = json_response(
        client
            .post(format!("{base}/upload/multipart/init/empty-multipart"))
            .bearer_auth(&token)
            .json(&json!({"total_size_bytes":0,"expected_parts":1}))
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let ev = empty["version_id"].as_str().unwrap();
    let es = empty["session_id"].as_str().unwrap();
    let _ = json_response(
        client
            .put(format!("{base}/upload/multipart/{ev}/0"))
            .bearer_auth(&token)
            .body(Vec::new())
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let status = json_response(
        client
            .get(format!("{base}/upload/multipart/{ev}"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(status["committed_parts"], 1);
    assert_eq!(status["committed_bytes"], 0);
    let _ = json_response(
        client
            .post(format!("{base}/upload/multipart/{ev}/{es}/finalize"))
            .bearer_auth(&token)
            .json(&json!({"total_size_bytes":0,"expected_parts":1}))
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let empty_read = client
        .get(format!("{base}/objects/get/empty-multipart"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(empty_read.status(), 200);
    assert!(empty_read.bytes().await.unwrap().is_empty());
    let excessive = client
        .post(format!("{base}/upload/multipart/init/too-many-parts"))
        .bearer_auth(&token)
        .json(&json!({"total_size_bytes":131073,"expected_parts":131073,"part_size_bytes":1}))
        .send()
        .await
        .unwrap();
    assert_eq!(excessive.status(), 413);
    // Simulate physical finalize succeeding before the metadata publication.
    let r = json_response(
        client
            .post(format!(
                "{base}/upload/multipart/init/recover-publication.bin"
            ))
            .bearer_auth(&token)
            .json(&json!({"total_size_bytes":4096,"expected_parts":1,"part_size_bytes":4096}))
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let rv = r["version_id"].as_str().unwrap();
    let _ = json_response(
        client
            .put(format!("{base}/upload/multipart/{rv}/0"))
            .bearer_auth(&token)
            .body(bytes[..4096].to_vec())
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    drop(stack.metadata.take());
    let ticket = {
        let db = m1::MetadataEngine::open(
            stack.root.join("metadata-control/metadata"),
            m1::Options::default(),
        )
        .unwrap();
        let fragment_id = n1_link::PoolId(
            ulid::Ulid::from_string(created["fragment_id"].as_str().unwrap())
                .unwrap()
                .to_bytes(),
        );
        let pending: Value = serde_json::from_slice(
            &db.snapshot()
                .get(
                    m1::Table::Uploads,
                    &m1::RecordKey::new(fragment_id, rv).unwrap(),
                )
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        pending["upload"]["object_key"].as_str().unwrap().to_owned()
    };
    let credentials: Value = serde_json::from_slice(
        &std::fs::read(stack.object_config.with_extension("credentials.json")).unwrap(),
    )
    .unwrap();
    let mut native_object =
        n1_native_client::N1NativeClient::connect(credentials["native_url"].as_str().unwrap())
            .await
            .unwrap();
    native_object
        .auth_fragment(
            credentials["fragment"].as_str().unwrap(),
            credentials["secret"].as_str().unwrap(),
        )
        .await
        .unwrap();
    native_object
        .control(n1_link::noa::Command::UploadFinalize { object_key: ticket })
        .await
        .unwrap();
    drop(native_object);
    stack.start_metadata().await;
    let recovered = client
        .get(format!("{base}/objects/get/recover-publication.bin"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(recovered.status(), 200);
    assert_eq!(recovered.bytes().await.unwrap().as_ref(), &bytes[..4096]);
    for format in ["tar", "zip"] {
        let export=json_response(client.post(format!("{base}/exports")).bearer_auth(&token).json(&json!({"keys":["native.txt","resume.bin"],"format":format,"filename":format!("witness.{format}")})).send().await.unwrap(),202).await;
        let id = export["export_id"].as_str().unwrap();
        let mut ready = false;
        for _ in 0..600 {
            let status = json_response(
                client
                    .get(format!("{base}/exports/{id}"))
                    .bearer_auth(&token)
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await;
            if status["phase"] == "failed" {
                panic!("export failed: {status}");
            }
            if status["phase"] == "ready" {
                ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(ready, "export never became ready");
        let response = client
            .get(format!("{base}/exports/{id}/download"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let archive = response.bytes().await.unwrap();
        assert!(
            archive
                .windows(b"native metadata to LINK".len())
                .any(|b| b == b"native metadata to LINK")
        );
        assert!(archive.windows(bytes.len()).any(|b| b == bytes));
        if format == "zip" {
            assert_eq!(&archive[..4], b"PK\x03\x04");
        } else {
            assert!(
                archive
                    .windows(b"native.txt".len())
                    .any(|b| b == b"native.txt")
            );
        }
        let signed = json_response(
            client
                .post(format!("{base}/exports/{id}/signed-url"))
                .bearer_auth(&token)
                .json(&json!({"expires_in":300}))
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        let suffix = client
            .get(signed["url"].as_str().unwrap())
            .header("range", "bytes=-97")
            .send()
            .await
            .unwrap();
        assert_eq!(suffix.status(), 206);
        assert_eq!(
            suffix.bytes().await.unwrap().as_ref(),
            &archive[archive.len() - 97..]
        );
    }
    std::fs::write(stack.root.join("PASSED.json"),serde_json::to_vec_pretty(&json!({
        "passed": true,
        "scope": "public HTTP and original native metadata to LINK; real process restarts, immutable versions, range, multipart bridge, grouped batch, permissions, trash/recovery, signed reads, refresh reuse, retained ZIP64/TAR export builder and signed ranged artifact downloads",
        "fragment": fragment
    })).unwrap()).unwrap();
}
