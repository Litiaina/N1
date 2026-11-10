//! Opt-in matched workload for the RC.2 preparation checkpoint. The caller
//! selects an optimized M1 binary; N1, geometry and policy stay identical.
use super::*;
use futures_util::{StreamExt, TryStreamExt};
use sha2::{Digest, Sha256};
use std::time::Instant;

fn resources(pid: u32) -> Value {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
    let kib = |key: &str| {
        status
            .lines()
            .find_map(|line| line.strip_prefix(key))
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    let fields: Vec<_> = stat
        .rsplit_once(')')
        .unwrap()
        .1
        .split_whitespace()
        .collect();
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
    json!({"user_seconds":fields[11].parse::<u64>().unwrap() as f64/ticks,
        "system_seconds":fields[12].parse::<u64>().unwrap() as f64/ticks,
        "rss_kib":kib("VmRSS:"),"peak_rss_kib":kib("VmHWM:")})
}

fn distribution(mut samples: Vec<f64>) -> Value {
    let completion_order_ms = samples.clone();
    samples.sort_by(f64::total_cmp);
    let percentile = |p: f64| samples[(samples.len() as f64 * p).ceil() as usize - 1];
    json!({"samples":samples.len(),"sum_ms":samples.iter().sum::<f64>(),
        "p50_ms":percentile(0.5),"p95_ms":percentile(0.95),"p99_ms":percentile(0.99),
        "completion_order_ms":completion_order_ms})
}

fn source(part: u64, bytes: usize) -> Vec<u8> {
    let mut output = vec![0u8; bytes];
    for (index, value) in output.iter_mut().enumerate() {
        *value = ((index as u64 + part * 17) % 251) as u8;
    }
    output[..8].copy_from_slice(&part.to_be_bytes());
    output
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit matched optimized binaries, private HDD roots and output path required"]
async fn matched_named_upload_read_and_mutation_measurement() {
    let output = PathBuf::from(
        std::env::var_os("N1_PREPARATION_BENCH_OUTPUT").expect("explicit benchmark output"),
    );
    assert!(!output.exists(), "never overwrite a measurement");
    let mut stack = Stack::with_block_bytes(262144).await;
    let object_config: Value =
        serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
    let layout = &object_config["pools"][0]["layout"];
    let ec = format!(
        "{}+{}",
        layout["data_shards"].as_u64().unwrap(),
        layout["parity_shards"].as_u64().unwrap()
    );
    drop(stack.metadata.take());
    let path = stack.root.join("m1_config.ini");
    let mut ini = ini::Ini::load_from_file(&path).unwrap();
    // No admission rejection is intended in a latency measurement. The same
    // configured quotas are enabled for both binaries (the old binary ignored
    // them); the candidate therefore includes its new enforcement cost.
    let mut section = ini.with_section(Some("rate_limiter_config"));
    section.set("enable_rate_limiting", "true");
    for class in ["auth", "global", "view", "upload"] {
        section
            .set(format!("{class}_limit_per_second"), "10000")
            .set(format!("{class}_limit_burst_size"), "10000");
    }
    ini.write_to_file(path).unwrap();
    stack.start_metadata().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(300))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token, _) = maintenance::auth(&client, &base, "preparation-measurement").await;
    let small = source(0, 4096);
    let first = resources(stack.object.as_ref().unwrap().0.id());
    let metadata_first = resources(stack.metadata.as_ref().unwrap().0.id());
    let started = Instant::now();
    let mut upload = Vec::new();
    for index in 0..100 {
        let now = Instant::now();
        json_response(
            client
                .post(format!(
                    "{base}/upload/one-shot/small-{index:03}.bin?durability=strict"
                ))
                .bearer_auth(&token)
                .body(small.clone())
                .send()
                .await
                .unwrap(),
            201,
        )
        .await;
        upload.push(now.elapsed().as_secs_f64() * 1000.0);
    }
    let mut read = Vec::new();
    for index in 0..100 {
        let now = Instant::now();
        let response = client
            .get(format!("{base}/objects/get/small-{index:03}.bin"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.bytes().await.unwrap().as_ref(), small);
        read.push(now.elapsed().as_secs_f64() * 1000.0);
    }
    const PART_BYTES: usize = 8 << 20;
    const PARTS: u64 = 32;
    let mut expected = Sha256::new();
    // Fixed 256 MiB client-side fixture, prepared before the large-upload timer.
    // This avoids measuring debug-harness byte generation as server throughput.
    let payloads: Vec<bytes::Bytes> = (0..PARTS)
        .map(|part| {
            let bytes = bytes::Bytes::from(source(part, PART_BYTES));
            expected.update(&bytes);
            bytes
        })
        .collect();
    let large_start = Instant::now();
    let now = Instant::now();
    let session = json_response(client.post(format!("{base}/upload/multipart/durable/init/large.bin?durability=strict"))
        .bearer_auth(&token).json(&json!({"total_size_bytes":PARTS*PART_BYTES as u64,"expected_parts":PARTS,"part_size_bytes":PART_BYTES}))
        .send().await.unwrap(),200).await;
    let init_ms = now.elapsed().as_secs_f64() * 1000.0;
    let version = session["version_id"].as_str().unwrap();
    let part_samples: Vec<f64> = futures_util::stream::iter(0..PARTS)
        .map(|part| {
            let client = client.clone();
            let token = &token;
            let base = &base;
            let payload = payloads[part as usize].clone();
            async move {
                let now = Instant::now();
                json_response(
                    client
                        .put(format!(
                            "{base}/upload/multipart/durable/{version}/{part}?durability=strict"
                        ))
                        .bearer_auth(token)
                        .body(payload)
                        .send()
                        .await
                        .unwrap(),
                    200,
                )
                .await;
                Ok::<_, std::io::Error>(now.elapsed().as_secs_f64() * 1000.0)
            }
        })
        .buffer_unordered(4)
        .try_collect()
        .await
        .unwrap();
    let now = Instant::now();
    json_response(
        client
            .post(format!(
                "{base}/upload/multipart/durable/{version}/{}/finalize",
                session["session_id"].as_str().unwrap()
            ))
            .bearer_auth(&token)
            .json(&json!({"total_size_bytes":PARTS*PART_BYTES as u64,"expected_parts":PARTS}))
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let finalize_ms = now.elapsed().as_secs_f64() * 1000.0;
    let upload_seconds = large_start.elapsed().as_secs_f64();
    let now = Instant::now();
    let mut response = client
        .get(format!("{base}/objects/get/large.bin"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let mut hash = Sha256::new();
    let mut downloaded = 0;
    while let Some(chunk) = response.chunk().await.unwrap() {
        hash.update(&chunk);
        downloaded += chunk.len();
    }
    assert_eq!(downloaded, PART_BYTES * PARTS as usize);
    assert_eq!(hash.finalize(), expected.finalize());
    let download_seconds = now.elapsed().as_secs_f64();
    let mut rename = Vec::new();
    let mut trash = Vec::new();
    for index in 0..100 {
        let now = Instant::now();
        json_response(client.post(format!("{base}/objects/rename")).bearer_auth(&token)
            .json(&json!({"source_key":format!("small-{index:03}.bin"),"destination_key":format!("renamed-{index:03}.bin")}))
            .send().await.unwrap(),200).await;
        rename.push(now.elapsed().as_secs_f64() * 1000.0);
        let now = Instant::now();
        json_response(
            client
                .delete(format!("{base}/objects/renamed-{index:03}.bin"))
                .bearer_auth(&token)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        trash.push(now.elapsed().as_secs_f64() * 1000.0);
    }
    let result = json!({"fixture":stack.root,"block_bytes":262144,"ec":ec,"rate_limiting_configured":true,
        "accel_configured":false,"small_bytes":4096,"small_objects":100,"large_bytes":PARTS*PART_BYTES as u64,
        "part_bytes":PART_BYTES,"part_concurrency":4,"wall_seconds":started.elapsed().as_secs_f64(),
        "small_upload":distribution(upload),"small_get":distribution(read),"rename":distribution(rename),"trash":distribution(trash),
        "large":{"upload_seconds":upload_seconds,"download_seconds":download_seconds,"init_ms":init_ms,"finalize_ms":finalize_ms,"parts":distribution(part_samples),"sha256_verified":true},
        "object_before":first,"metadata_before":metadata_first,"object_after":resources(stack.object.as_ref().unwrap().0.id()),
        "metadata_after":resources(stack.metadata.as_ref().unwrap().0.id())});
    std::fs::write(output, serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

fn existing_large_object(root: &Path) -> (String, String, PathBuf) {
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
    let rows = view.scan(m1::Table::Versions, id, "", None, 1000).unwrap();
    let stored = rows
        .into_iter()
        .find_map(|(_, bytes)| {
            let row: Value = serde_json::from_slice(&bytes).unwrap();
            (row["metadata"]["total_size_bytes"] == 268435456u64).then_some(row)
        })
        .expect("existing benchmark payload");
    let settings: Value =
        serde_json::from_slice(&std::fs::read(root.join("object.json")).unwrap()).unwrap();
    let layout: n1_link::PoolLayout =
        serde_json::from_value(settings["pools"][0]["layout"].clone()).unwrap();
    let roots: Vec<PathBuf> =
        serde_json::from_value(settings["pools"][0]["roots"].clone()).unwrap();
    let fingerprint = n1_link::ObjectKey::inspect(stored["object_key"].as_str().unwrap())
        .unwrap()
        .fingerprint();
    (
        fragment["alias"].as_str().unwrap().to_owned(),
        stored["metadata"]["version_id"]
            .as_str()
            .unwrap()
            .to_owned(),
        roots[layout.member_slot(fingerprint, 0).unwrap()].clone(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "replays one task-owned existing object across optimized binaries; explicit run root required"]
async fn identical_object_read_replay_and_placement_audit() {
    let run_root = PathBuf::from(
        std::env::var_os("N1_PREPARATION_RUN_ROOT").expect("explicit preparation run root"),
    );
    let output = std::env::var_os("N1_PREPARATION_REPLAY_OUTPUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| run_root.join("reports/replay-read.json"));
    assert!(!output.exists());
    let mut placements = Vec::new();
    let mut replay = None;
    for name in ["A1", "B1", "B2", "A2"] {
        let result: Value = serde_json::from_slice(
            &std::fs::read(run_root.join(format!("reports/benchmark-{name}.json"))).unwrap(),
        )
        .unwrap();
        let root = PathBuf::from(result["fixture"].as_str().unwrap());
        let (alias, version, member) = existing_large_object(&root);
        placements.push(json!({"sample":name,"data_member":member,"fixture":root}));
        if name == "A1" {
            replay = Some((root, alias, version));
        }
    }
    let (root, alias, version) = replay.unwrap();
    let config = m1::server::ServerConfig::load(&root.join("m1_config.ini")).unwrap();
    let mut stack = Stack {
        object: None,
        metadata: None,
        url: format!("http://{}", config.http),
        native: config.native.port(),
        object_binary: PathBuf::from(std::env::var_os("N1_OBJECT_TEST_BINARY").unwrap()),
        object_config: root.join("object.json"),
        root,
    };
    stack.start_object().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(180))
        .build()
        .unwrap();
    let mut expected = Sha256::new();
    for part in 0..32 {
        expected.update(source(part, 8 << 20));
    }
    let expected: [u8; 32] = expected.finalize().into();
    let mut samples = Vec::new();
    for sample in ["A1", "B1", "B2", "A2"] {
        let binary = if sample.starts_with('A') {
            run_root.join("artifacts/litiaina-m1-baseline")
        } else {
            std::env::var_os("M1_PREPARATION_REPLAY_CANDIDATE")
                .map(PathBuf::from)
                .unwrap_or_else(|| run_root.join("artifacts/litiaina-m1-candidate"))
        };
        stack.metadata = Some(spawn(
            &binary,
            &["serve"],
            &stack.root,
            "replay-metadata.log",
        ));
        let base = format!("{}/noa/v1", stack.url);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        loop {
            if client
                .get(format!("{base}/ping"))
                .send()
                .await
                .is_ok_and(|r| r.status() == 200)
            {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let login = json_response(
            client
                .post(format!("{base}/auth/fragment"))
                .json(&json!({"fragment":alias,"secret":"test-only-fragment","read":true}))
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        let token = login["access_token"].as_str().unwrap();
        let mut times = Vec::new();
        // One verified warmup per M1 restart, followed by two measured GETs.
        let mut metadata_before = Value::Null;
        let mut object_before = Value::Null;
        for iteration in 0..3 {
            if iteration == 1 {
                metadata_before = resources(stack.metadata.as_ref().unwrap().0.id());
                object_before = resources(stack.object.as_ref().unwrap().0.id());
            }
            let now = Instant::now();
            let mut response = client
                .get(format!("{base}/objects/get/large.bin"))
                .bearer_auth(token)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            assert_eq!(response.headers()["etag"], format!("\"{version}\""));
            let mut hash = Sha256::new();
            let mut length = 0;
            while let Some(bytes) = response.chunk().await.unwrap() {
                hash.update(&bytes);
                length += bytes.len();
            }
            assert_eq!(length, 268435456);
            assert_eq!(<[u8; 32]>::from(hash.finalize()), expected);
            if iteration > 0 {
                times.push(now.elapsed().as_secs_f64());
            }
        }
        samples.push(json!({"sample":sample,"read_seconds":times,"metadata_before":metadata_before,
            "metadata_after":resources(stack.metadata.as_ref().unwrap().0.id()),"object_before":object_before,
            "object_after":resources(stack.object.as_ref().unwrap().0.id())}));
        drop(stack.metadata.take());
    }
    std::fs::write(output,serde_json::to_vec_pretty(&json!({"placements":placements,"same_fixture":stack.root,"samples":samples,"verified":true})).unwrap()).unwrap();
}
