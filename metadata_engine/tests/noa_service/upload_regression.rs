//! Controlled comparison: one N1 process, one M1 database and alternating
//! binaries/policies. Raw samples are retained so disk stalls remain visible.
use super::*;
use std::time::Instant;

async fn stop_metadata(stack: &mut Stack) {
    let Some(mut process) = stack.metadata.take() else {
        return;
    };
    assert_eq!(
        unsafe { libc::kill(process.0.id() as i32, libc::SIGTERM) },
        0
    );
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        if let Some(status) = process.0.try_wait().unwrap() {
            assert!(status.success(), "M1 must shut down cleanly: {status}");
            return;
        }
        assert!(Instant::now() < deadline, "M1 shutdown timed out");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn configure_quotas(stack: &Stack, enabled: bool) {
    let path = stack.root.join("m1_config.ini");
    let mut ini = ini::Ini::load_from_file(&path).unwrap();
    let mut section = ini.with_section(Some("rate_limiter_config"));
    section.set("enable_rate_limiting", enabled.to_string());
    for class in ["auth", "global", "view", "upload"] {
        section
            .set(format!("{class}_limit_per_second"), "10000")
            .set(format!("{class}_limit_burst_size"), "10000");
    }
    ini.write_to_file(path).unwrap();
}

async fn start_variant(stack: &mut Stack, client: &Client, binary: &Path) {
    stack.metadata = Some(spawn(binary, &["serve"], &stack.root, "metadata.log"));
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if client
            .get(format!("{}/noa/v1/ping", stack.url))
            .send()
            .await
            .is_ok_and(|r| r.status() == 200)
        {
            return;
        }
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn resources(pid: u32) -> Value {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    let fields: Vec<_> = stat
        .rsplit_once(')')
        .unwrap()
        .1
        .split_whitespace()
        .collect();
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
    let memory = |key| {
        status
            .lines()
            .find_map(|s| s.strip_prefix(key))
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    json!({"user_seconds":fields[11].parse::<u64>().unwrap() as f64/ticks,
        "system_seconds":fields[12].parse::<u64>().unwrap() as f64/ticks,
        "rss_kib":memory("VmRSS:"),"peak_rss_kib":memory("VmHWM:")})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit optimized binaries, private roots and output path required"]
async fn alternating_small_uploads_on_one_database_and_pool() {
    let output = PathBuf::from(std::env::var_os("M1_UPLOAD_BENCH_OUTPUT").unwrap());
    assert!(!output.exists(), "never overwrite evidence");
    let baseline = PathBuf::from(std::env::var_os("M1_UPLOAD_BASELINE").unwrap());
    let candidate = PathBuf::from(std::env::var_os("M1_UPLOAD_CANDIDATE").unwrap());
    let count: usize = std::env::var("M1_UPLOAD_SAMPLES")
        .unwrap_or("32".into())
        .parse()
        .unwrap();
    assert!(count > 0);
    let mut stack = Stack::with_block_bytes(262144).await;
    let client = Client::builder()
        .timeout(Duration::from_secs(180))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token, _) = maintenance::auth(&client, &base, "upload-regression").await;
    stop_metadata(&mut stack).await;
    let high_object_quotas = std::env::var_os("M1_UPLOAD_HIGH_OBJECT_QUOTAS").is_some();
    if high_object_quotas {
        // A software-only run can reach the independent N1 policy ceiling.
        // Raise its configured quotas identically for every measured variant;
        // a rejected request must never be counted as completed benchmark work.
        drop(stack.object.take());
        let mut settings: Value =
            serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
        for class in ["auth", "view", "upload", "global"] {
            settings["rate_limits"][format!("{class}_per_sec")] = json!(10000);
            settings["rate_limits"][format!("{class}_burst")] = json!(10000);
        }
        std::fs::write(
            &stack.object_config,
            serde_json::to_vec_pretty(&settings).unwrap(),
        )
        .unwrap();
        stack.start_object().await;
    }
    let payload = bytes::Bytes::from(vec![0x53; 4096]);
    let mut phases = Vec::new();
    // Four observations per variant; reverse each block to counter time drift.
    for (phase, variant) in ["A", "B", "C", "C", "B", "A", "A", "B", "C", "C", "B", "A"]
        .into_iter()
        .enumerate()
    {
        configure_quotas(&stack, variant != "C");
        let binary = if variant == "A" {
            &baseline
        } else {
            &candidate
        };
        start_variant(&mut stack, &client, binary).await;
        let mut samples = Vec::new();
        let mut metadata_before = Value::Null;
        let mut object_before = Value::Null;
        for index in 0..count + 3 {
            if index == 3 {
                metadata_before = resources(stack.metadata.as_ref().unwrap().0.id());
                object_before = resources(stack.object.as_ref().unwrap().0.id());
            }
            let name = format!("p{phase:02}-n{index:04}.bin");
            let now = Instant::now();
            let response = json_response(
                client
                    .post(format!("{base}/upload/one-shot/{name}?durability=strict"))
                    .bearer_auth(&token)
                    .body(payload.clone())
                    .send()
                    .await
                    .unwrap(),
                201,
            )
            .await;
            if index >= 3 {
                samples.push(json!({"name":name,"milliseconds":now.elapsed().as_secs_f64()*1000.0,"version":response["version_id"]}));
            }
        }
        let mut result = json!({"phase":phase,"variant":variant,"binary":binary,"rate_limits":variant!="C",
            "samples":samples,"metadata_before":metadata_before,"object_before":object_before,
            "metadata_after":resources(stack.metadata.as_ref().unwrap().0.id()),"object_after":resources(stack.object.as_ref().unwrap().0.id())});
        eprintln!(
            "phase {phase} {variant}: {} ms",
            samples
                .iter()
                .map(|s| s["milliseconds"].as_f64().unwrap())
                .sum::<f64>()
        );
        // Outside the upload timer: verify every ACK through the public API.
        let mut reads = Vec::new();
        for index in 0..count + 3 {
            let now = Instant::now();
            let response = client
                .get(format!("{base}/objects/get/p{phase:02}-n{index:04}.bin"))
                .bearer_auth(&token)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            assert_eq!(response.bytes().await.unwrap(), payload);
            if index >= 3 {
                reads.push(now.elapsed().as_secs_f64() * 1000.0);
            }
        }
        result["verified_get_ms"] = json!(reads);
        stop_metadata(&mut stack).await;
        phases.push(result);
        std::fs::write(
            &output,
            serde_json::to_vec_pretty(&json!({"fixture":stack.root,"bytes":4096,"strict":true,
            "same_n1_pid":stack.object.as_ref().unwrap().0.id(),"phases":phases,"verified":true}))
            .unwrap(),
        )
        .unwrap();
    }
    audit_placements(&stack.root, &mut phases);
    std::fs::write(
        output,
        serde_json::to_vec_pretty(&json!({"fixture":stack.root,"bytes":4096,"strict":true,
        "same_n1_pid":stack.object.as_ref().unwrap().0.id(),"high_object_quotas":high_object_quotas,
        "phases":phases,"verified":true,"complete":true}))
        .unwrap(),
    )
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "isolates rename/trash software and durable transaction latency; explicit binaries/roots required"]
async fn alternating_namespace_mutations_on_one_database() {
    let output = PathBuf::from(std::env::var_os("M1_MUTATION_BENCH_OUTPUT").unwrap());
    assert!(!output.exists());
    let baseline = PathBuf::from(std::env::var_os("M1_UPLOAD_BASELINE").unwrap());
    let candidate = PathBuf::from(std::env::var_os("M1_UPLOAD_CANDIDATE").unwrap());
    let count: usize = std::env::var("M1_UPLOAD_SAMPLES")
        .unwrap_or("32".into())
        .parse()
        .unwrap();
    assert!(count > 0 && count.is_multiple_of(2));
    let mut stack = Stack::with_block_bytes(262144).await;
    let client = Client::builder()
        .timeout(Duration::from_secs(180))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token, _) = maintenance::auth(&client, &base, "namespace-regression").await;
    let payload = bytes::Bytes::from(vec![0x71; 4096]);
    for key in ["rename-a.bin", "trash.bin"] {
        json_response(
            client
                .post(format!("{base}/upload/one-shot/{key}?durability=strict"))
                .bearer_auth(&token)
                .body(payload.clone())
                .send()
                .await
                .unwrap(),
            201,
        )
        .await;
    }
    stop_metadata(&mut stack).await;
    let mut phases = Vec::new();
    for (phase, variant) in ["A", "B", "C", "C", "B", "A", "A", "B", "C", "C", "B", "A"]
        .into_iter()
        .enumerate()
    {
        configure_quotas(&stack, variant != "C");
        let binary = if variant == "A" {
            &baseline
        } else {
            &candidate
        };
        start_variant(&mut stack, &client, binary).await;
        let mut rename = Vec::new();
        let mut trash = Vec::new();
        let before = resources(stack.metadata.as_ref().unwrap().0.id());
        // Alternate the exact same source/destination. Trashing/recovery also
        // uses one fixed logical object; no payload write belongs to this timer.
        for index in 0..count + 4 {
            let (source, destination) = if index.is_multiple_of(2) {
                ("rename-a.bin", "rename-b.bin")
            } else {
                ("rename-b.bin", "rename-a.bin")
            };
            let now = Instant::now();
            json_response(
                client
                    .post(format!("{base}/objects/rename"))
                    .bearer_auth(&token)
                    .json(&json!({"source_key":source,"destination_key":destination}))
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await;
            if index >= 4 {
                rename.push(now.elapsed().as_secs_f64() * 1000.0);
            }
        }
        for index in 0..count + 4 {
            let now = Instant::now();
            json_response(
                client
                    .delete(format!("{base}/objects/trash.bin"))
                    .bearer_auth(&token)
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await;
            if index >= 4 {
                trash.push(now.elapsed().as_secs_f64() * 1000.0);
            }
            json_response(
                client
                    .post(format!("{base}/objects/recover/trash.bin"))
                    .bearer_auth(&token)
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await;
        }
        let result = json!({"phase":phase,"variant":variant,"binary":binary,"rename_ms":rename,"trash_ms":trash,
            "metadata_before":before,"metadata_after":resources(stack.metadata.as_ref().unwrap().0.id())});
        eprintln!(
            "phase {phase} {variant}: rename {:.3} ms, trash {:.3} ms",
            rename.iter().sum::<f64>() / count as f64,
            trash.iter().sum::<f64>() / count as f64
        );
        for key in ["rename-a.bin", "trash.bin"] {
            let response = client
                .get(format!("{base}/objects/get/{key}"))
                .bearer_auth(&token)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            assert_eq!(response.bytes().await.unwrap(), payload);
        }
        stop_metadata(&mut stack).await;
        phases.push(result);
        std::fs::write(
            &output,
            serde_json::to_vec_pretty(
                &json!({"fixture":stack.root,"phases":phases,"verified":true,"complete":phase==11}),
            )
            .unwrap(),
        )
        .unwrap();
    }
}

// The Versions keyspace also contains secondary indexes; only stored-version
// rows carry a physical object key. Inspect them after M1 releases its DB lock.
fn audit_placements(root: &Path, phases: &mut [Value]) {
    let config = m1::server::ServerConfig::load(&root.join("m1_config.ini")).unwrap();
    let db = m1::MetadataEngine::open(config.data_dir, m1::Options::default()).unwrap();
    let view = db.snapshot();
    let fragments = view
        .scan(
            m1::Table::Reclamation,
            n1_link::PoolId([0; 16]),
            "fragment/",
            None,
            100,
        )
        .unwrap();
    assert_eq!(fragments.len(), 1);
    let fragment: Value = serde_json::from_slice(&fragments[0].1).unwrap();
    let id = serde_json::from_value(fragment["id"].clone()).unwrap();
    let settings: Value =
        serde_json::from_slice(&std::fs::read(root.join("object.json")).unwrap()).unwrap();
    let layout: n1_link::PoolLayout =
        serde_json::from_value(settings["pools"][0]["layout"].clone()).unwrap();
    let mut cursor = None;
    let mut slots = std::collections::HashMap::new();
    loop {
        let rows = view
            .scan(m1::Table::Versions, id, "", cursor.as_ref(), 1000)
            .unwrap();
        if rows.is_empty() {
            break;
        }
        cursor = rows.last().map(|(key, _)| key.clone());
        for (_, bytes) in rows {
            let row: Value = serde_json::from_slice(&bytes).unwrap();
            let Some(key) = row["object_key"].as_str() else {
                continue;
            };
            let key = n1_link::ObjectKey::inspect(key).unwrap();
            slots.insert(
                row["metadata"]["version_id"].as_str().unwrap().to_owned(),
                layout.member_slot(key.fingerprint(), 0).unwrap(),
            );
        }
    }
    for phase in phases {
        for sample in phase["samples"].as_array_mut().unwrap() {
            sample["data_member_slot"] = json!(slots[sample["version"].as_str().unwrap()]);
        }
    }
}

#[test]
#[ignore = "audits previously captured task-owned samples without repeating or overwriting them"]
fn audit_saved_upload_placements() {
    let input = PathBuf::from(std::env::var_os("M1_UPLOAD_AUDIT_INPUT").unwrap());
    let output = PathBuf::from(std::env::var_os("M1_UPLOAD_AUDIT_OUTPUT").unwrap());
    assert!(!output.exists());
    let mut result: Value = serde_json::from_slice(&std::fs::read(input).unwrap()).unwrap();
    assert_eq!(
        result["phases"].as_array().unwrap().len(),
        12,
        "partial campaigns cannot pass the final audit"
    );
    let root = PathBuf::from(result["fixture"].as_str().unwrap());
    audit_placements(&root, result["phases"].as_array_mut().unwrap());
    result["placement_audit_complete"] = json!(true);
    std::fs::write(output, serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real optimized M1/N1, explicit HDD roots and openssl certificate fixture required"]
async fn tls_listener_preserves_named_upload_and_streamed_read() {
    let mut stack = Stack::with_block_bytes(262144).await;
    stop_metadata(&mut stack).await;
    let cert = stack.root.join("test-cert.pem");
    let key = stack.root.join("test-key.pem");
    assert!(
        Command::new("openssl")
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
                "-out"
            ])
            .arg(&cert)
            .arg("-keyout")
            .arg(&key)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    let path = stack.root.join("m1_config.ini");
    let mut ini = ini::Ini::load_from_file(&path).unwrap();
    ini.with_section(Some("server"))
        .set("cert_path", cert.to_str().unwrap())
        .set("key_path", key.to_str().unwrap());
    ini.write_to_file(path).unwrap();
    let binary = PathBuf::from(std::env::var_os("M1_TEST_BINARY").unwrap());
    stack.metadata = Some(spawn(&binary, &["serve"], &stack.root, "metadata-tls.log"));
    let client = Client::builder()
        .timeout(Duration::from_secs(60))
        .add_root_certificate(
            reqwest::Certificate::from_pem(&std::fs::read(cert).unwrap()).unwrap(),
        )
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url.replacen("http://", "https://", 1));
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if client
            .get(format!("{base}/ping"))
            .send()
            .await
            .is_ok_and(|r| r.status() == 200)
        {
            break;
        }
        assert!(Instant::now() < deadline, "TLS listener must become ready");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let (_, token, _) = maintenance::auth(&client, &base, "tls-transport").await;
    let payload = vec![0x37; 256 * 1024 + 17];
    let published = json_response(
        client
            .post(format!("{base}/upload/one-shot/tls.bin?durability=strict"))
            .bearer_auth(&token)
            .body(payload.clone())
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
    let response = client
        .get(format!("{base}/objects/get/tls.bin"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.headers()["etag"],
        format!("\"{}\"", published["version_id"].as_str().unwrap())
    );
    assert_eq!(response.bytes().await.unwrap().as_ref(), payload);
    let response = client
        .get(format!("{base}/objects/get/tls.bin"))
        .bearer_auth(&token)
        .header("range", "bytes=17-4096")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 206);
    assert_eq!(response.bytes().await.unwrap().as_ref(), &payload[17..4097]);
    stop_metadata(&mut stack).await;
}
