//! Real HTTPS Accel, process-local CA trust, hostname rejection and restart.
//! No global trust-store changes or disabled certificate verification.
use super::*;

fn openssl(root: &Path, args: &[&str]) {
    assert!(
        Command::new("openssl")
            .args(args)
            .current_dir(root)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
}

fn leaf(root: &Path, ip: &str) {
    openssl(
        root,
        &[
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-subj",
            "/CN=isolated-accel",
            "-keyout",
            "server.key",
            "-out",
            "server.csr",
        ],
    );
    std::fs::write(root.join("server.ext"), format!(
        "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectAltName=IP:{ip}\n")).unwrap();
    openssl(
        root,
        &[
            "x509",
            "-req",
            "-in",
            "server.csr",
            "-CA",
            "ca.pem",
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-days",
            "1",
            "-extfile",
            "server.ext",
            "-out",
            "server.pem",
        ],
    );
    for name in ["server.key", "ca.key"] {
        std::fs::set_permissions(root.join(name), std::fs::Permissions::from_mode(0o600)).unwrap();
    }
}

async fn read(client: &Client, base: &str, token: &str, bytes: &[u8], cached: bool) {
    let response = client
        .get(format!("{base}/objects/get/tls.bin"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers().contains_key("x-accel-cache"), cached);
    assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
    let response = client
        .get(format!("{base}/namespace/search?q=tls.bin"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers().contains_key("x-accel-search"), cached);
    assert_eq!(response.headers().contains_key("x-m1-search"), !cached);
    let result: Value = response.json().await.unwrap();
    assert_eq!(result["results"][0]["object_key"], "tls.bin");
}

async fn start(binary: &Path, root: &Path, port: u16) -> Process {
    let check = Command::new(binary)
        .arg("--check-config")
        .current_dir(root)
        .output()
        .unwrap();
    assert!(
        check.status.success(),
        "{}",
        String::from_utf8_lossy(&check.stderr)
    );
    let mut process = spawn(binary, &[], root, "accel.log");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        assert!(
            process.0.try_wait().unwrap().is_none(),
            "{}",
            std::fs::read_to_string(root.join("accel.log")).unwrap()
        );
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return process;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Accel TLS listener did not start"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real Accel HTTPS and process-local private CA on isolated N1/M1 fixtures"]
async fn private_ca_hostname_validation_and_accel_restart_preserve_authoritative_reads() {
    let mut stack = Stack::with_block_bytes(262144).await;
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token, _) = maintenance::auth(&client, &base, "Accel TLS").await;
    let bytes = vec![0x63; 262144 + 17];
    maintenance::put(&client, &base, &token, "tls.bin", &bytes).await;
    let root = stack.root.join("accel-tls");
    std::fs::create_dir(&root).unwrap();
    openssl(
        &root,
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=isolated-accel-ca",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
        ],
    );
    leaf(&root, "127.0.0.1");
    let accel_port = port();
    let metadata_env = std::fs::read_to_string(stack.root.join("m1.env")).unwrap();
    let jwt = metadata_env
        .lines()
        .find_map(|s| s.strip_prefix("JWT_SECRET="))
        .unwrap();
    std::fs::write(root.join("accel_config.ini"), format!(
        "[server]\nbind=127.0.0.1:{accel_port}\ntls_cert=server.pem\ntls_key=server.key\n\
        [cache]\ncapacity_bytes=16777216\nmax_entry_bytes=8388608\nttl_seconds=3600\nidle_seconds=900\nmemory_budget_bytes=67108864\nblock_bytes=262144\nbody_idle_timeout_ms=10000\nfill_wait_timeout_ms=10000\nmax_active_requests=16\n\
        [search]\ncapacity_bytes=8388608\nmax_fragment_entries=10000\nrefresh_seconds=30\norigin_idle_timeout_ms=10000\n\
        [security]\nadmin_key_env=ACCEL_ADMIN_KEY\nservice_key_env=ACCEL_SERVICE_KEY\n\
        [backend:n1-primary]\nenabled=true\nengine=n1\norigin={}\njwt_secret_env=ACCEL_N1_JWT_SECRET\nallow_invalid_tls=false\ncache_private_responses=true\nconnect_timeout_ms=1000\nheader_timeout_ms=10000\nqueue_timeout_ms=1000\nmax_inflight_requests=16\n", stack.url)).unwrap();
    std::fs::write(root.join("accel.env"), format!(
        "ACCEL_ADMIN_KEY=isolated-test-admin-key\nACCEL_SERVICE_KEY=isolated-test-service-key\nACCEL_N1_JWT_SECRET={jwt}\n")).unwrap();
    std::fs::set_permissions(
        root.join("accel.env"),
        std::fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    let binary = PathBuf::from(std::env::var_os("N1_ACCEL_TEST_BINARY").unwrap());
    let mut accel = Some(start(&binary, &root, accel_port).await);
    drop(stack.metadata.take());
    std::fs::write(stack.root.join("m1.env"), format!(
        "{metadata_env}N1_ACCEL_READ_URL=https://127.0.0.1:{accel_port}\nN1_ACCEL_SEARCH_URL=https://127.0.0.1:{accel_port}\n")).unwrap();
    stack.start_metadata().await;
    read(&client, &base, &token, &bytes, false).await;
    let ca = root.join("ca.pem");
    drop(stack.metadata.take());
    stack
        .start_metadata_with_env(&[("SSL_CERT_FILE", ca.to_str().unwrap())])
        .await;
    read(&client, &base, &token, &bytes, true).await;
    // Exercise concurrent TLS cache callbacks with the same pinned payload.
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let (client, base, token, bytes) =
            (client.clone(), base.clone(), token.clone(), bytes.clone());
        tasks.push(tokio::spawn(async move {
            for _ in 0..8 {
                read(&client, &base, &token, &bytes, true).await;
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    drop(accel.take());
    leaf(&root, "127.0.0.2");
    accel = Some(start(&binary, &root, accel_port).await);
    drop(stack.metadata.take());
    stack
        .start_metadata_with_env(&[("SSL_CERT_FILE", ca.to_str().unwrap())])
        .await;
    read(&client, &base, &token, &bytes, false).await;
    drop(accel.take());
    leaf(&root, "127.0.0.1");
    let _accel = start(&binary, &root, accel_port).await;
    read(&client, &base, &token, &bytes, true).await;
    reliability::record(
        &stack.root.join("ACCEL-TLS-PASSED.json"),
        &json!({
        "passed":true,"untrusted_ca_rejected":true,"wrong_hostname_rejected":true,
        "trusted_ca_cache_and_search_verified":true,"concurrent_pinned_reads":64,
        "cache_restart_verified":true,"origin_fallback_preserves_bytes":true,
        "global_trust_store_modified":false,"trust_setting":"process-local SSL_CERT_FILE"}),
    );
}
