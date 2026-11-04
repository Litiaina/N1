//! Matched transfer runs with and without concurrent monitoring traffic.
use super::*;
use sha2::{Digest, Sha256};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

struct ProcessSample {
    ticks: u64,
    start: u64,
    rss: u64,
}
fn process_sample(pid: u32) -> ProcessSample {
    let text = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    let fields = text[text.rfind(')').unwrap() + 1..]
        .split_whitespace()
        .collect::<Vec<_>>();
    let value = |i: usize| fields[i].parse::<u64>().unwrap();
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    assert!(page > 0);
    ProcessSample {
        ticks: value(11) + value(12),
        start: value(19),
        rss: value(21) * page as u64,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "six quiet-node phases measuring monitoring CPU, RSS and latency; explicit private root required"]
async fn monitoring_cpu_rss_latency_and_zero_payload_io() {
    let stack = Stack::new().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let fragment=json_response(client.post(format!("{base}/fragments")).json(&json!({"name":"Monitoring CPU witness","secret":"test-only-master","key":"test-only-fragment"})).send().await.unwrap(),201).await;
    let login=json_response(client.post(format!("{base}/auth/fragment")).json(&json!({"fragment":fragment["fragment_hash"],"secret":"test-only-fragment","read":true,"write":true,"modify":true,"delete":true})).send().await.unwrap(),200).await;
    let token = login["access_token"].as_str().unwrap().to_owned();
    // Exercise populated accounting shards, rather than measuring only an
    // empty fragment. These real uploads are outside the measurement window.
    for index in 0..1024 {
        json_response(
            client
                .post(format!(
                    "{base}/upload/one-shot/cpu-seed/{index}.bin?durability=strict"
                ))
                .bearer_auth(&token)
                .body(vec![41u8; 128])
                .send()
                .await
                .unwrap(),
            201,
        )
        .await;
        // Respect the unchanged service's 30/s view quota during fixture
        // creation. No rejection/retry is hidden inside the measurements.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let usage = json_response(
        client
            .get(format!("{base}/capacity/fragment"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(usage["object_count"], 1024);
    assert_eq!(usage["used_bytes"], 1024 * 128);
    let pids = [
        stack.metadata.as_ref().unwrap().0.id(),
        stack.object.as_ref().unwrap().0.id(),
    ];
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    assert!(hz > 0);
    let mut runs = Vec::new();
    for monitoring in [false, true, true, false, false, true] {
        let health = |client: Client, base: String, token: String| async move {
            json_response(
                client
                    .get(format!("{base}/maintenance/health?live_counters=true"))
                    .bearer_auth(token)
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await
        };
        let before = health(client.clone(), base.clone(), token.clone()).await;
        let first = pids.map(process_sample);
        let mut peak = first.each_ref().map(|s| s.rss);
        let running = Arc::new(AtomicBool::new(true));
        let mut watchers = Vec::new();
        if monitoring {
            for worker in 0..4 {
                let (client, base, token, running) =
                    (client.clone(), base.clone(), token.clone(), running.clone());
                watchers.push(tokio::spawn(async move {
                    let mut samples = Vec::new();
                    while running.load(Ordering::Relaxed) {
                        let suffix = match worker % 3 {
                            0 => "maintenance/health?refresh=true",
                            1 => "maintenance/pools/health?deep_scan=false",
                            _ => "capacity/fragment",
                        };
                        let start = std::time::Instant::now();
                        json_response(
                            client
                                .get(format!("{base}/{suffix}"))
                                .bearer_auth(&token)
                                .send()
                                .await
                                .unwrap(),
                            200,
                        )
                        .await;
                        samples.push(start.elapsed().as_secs_f64() * 1000.);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    samples
                }));
            }
        }
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            for (index, pid) in pids.iter().enumerate() {
                peak[index] = peak[index].max(process_sample(*pid).rss);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        running.store(false, Ordering::Relaxed);
        let mut latencies = Vec::new();
        for watcher in watchers {
            latencies.extend(watcher.await.unwrap());
        }
        let seconds = start.elapsed().as_secs_f64();
        let last = pids.map(process_sample);
        let after = health(client.clone(), base.clone(), token.clone()).await;
        assert_eq!(
            before["io"]["backend"]["submitted"]
                .as_u64()
                .expect("missing live backend counter"),
            after["io"]["backend"]["submitted"]
                .as_u64()
                .expect("missing live backend counter"),
            "monitoring submitted object payload I/O"
        );
        let nodes = pids.iter().enumerate().map(|(i,pid)| {
            assert_eq!(first[i].start,last[i].start,"measured process changed");
            let cpu_seconds = (last[i].ticks-first[i].ticks)as f64/hz as f64;
            json!({"role":if i==0 {"metadata"}else{"object"},"pid":pid,"cpu_seconds":cpu_seconds,"cpu_one_core_percent":100.*cpu_seconds/seconds,"sampled_peak_rss_bytes":peak[i],"rss_before_bytes":first[i].rss,"rss_after_bytes":last[i].rss})
        }).collect::<Vec<_>>();
        latencies.sort_by(f64::total_cmp);
        let quantile = |percent: usize| {
            if latencies.is_empty() {
                None
            } else {
                Some(
                    latencies[(latencies.len() * percent)
                        .div_ceil(100)
                        .saturating_sub(1)
                        .min(latencies.len() - 1)],
                )
            }
        };
        runs.push(json!({"monitoring":monitoring,"seconds":seconds,"requests":latencies.len(),"latency_p50_ms":quantile(50),"latency_p95_ms":quantile(95),"latency_p99_ms":quantile(99),"nodes":nodes,"metadata_samples_before":before["components"]["metadata"]["observation"]["sequence"],"metadata_samples_after":after["components"]["metadata"]["observation"]["sequence"],"object_payload_io_submitted_delta":0}));
    }
    std::fs::write(stack.root.join("MONITORING-CPU-RSS.json"),serde_json::to_vec_pretty(&json!({"passed":true,"scope":"private quiet service processes; control-plane CPU/RSS, not HDD transfer throughput","seed_objects":1024,"object_monitor_interval_seconds":1,"order":"A B B A A B","clients":4,"poll_millis":100,"resource_sample_millis":100,"metadata_binary":env!("CARGO_BIN_EXE_litiaina-n1-metadata"),"object_binary":stack.object_binary,"runs":runs})).unwrap()).unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "six matched 256 MiB two-HDD transfers with monitoring pressure"]
async fn matched_monitoring_overhead_upload_download_integrity_and_no_scan_witness() {
    let mut stack = Stack::with_block_bytes(262144).await;
    drop(stack.metadata.take());
    drop(stack.object.take());
    let mut settings: Value =
        serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
    settings["runtime"]["monitor_interval_seconds"] = json!(30);
    std::fs::write(&stack.object_config, serde_json::to_vec(&settings).unwrap()).unwrap();
    let config = stack.root.join("n1_config.ini");
    let mut ini = ini::Ini::load_from_file(&config).unwrap();
    ini.with_section(Some("upload_config"))
        .set("monitor_interval_seconds", "30");
    ini.write_to_file(config).unwrap();
    stack.start_object().await;
    stack.start_metadata().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(300))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let fragment=json_response(client.post(format!("{base}/fragments")).json(&json!({"name":"Monitoring overhead","secret":"test-only-master","key":"test-only-fragment"})).send().await.unwrap(),201).await;
    let login=json_response(client.post(format!("{base}/auth/fragment")).json(&json!({"fragment":fragment["fragment_hash"],"secret":"test-only-fragment","read":true,"write":true,"modify":true,"delete":true})).send().await.unwrap(),200).await;
    let token = login["access_token"].as_str().unwrap().to_owned();
    // The part generator is outside transfer timings and has fixed memory.
    let part = (0..8 * 1024 * 1024)
        .map(|n| (n % 251) as u8)
        .collect::<Vec<_>>();
    let parts = 32u64;
    let mut hash = Sha256::new();
    for _ in 0..parts {
        hash.update(&part);
    }
    let expected = hash.finalize();
    let mut results = Vec::new();
    for (run, monitoring) in [false, true, true, false, false, true]
        .into_iter()
        .enumerate()
    {
        let running = Arc::new(AtomicBool::new(true));
        let requests = Arc::new(AtomicU64::new(0));
        let mut watchers = Vec::new();
        let warm = json_response(
            client
                .get(format!("{base}/maintenance/health"))
                .bearer_auth(&token)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        if monitoring {
            for worker in 0..4 {
                let (client, base, token, running, requests) = (
                    client.clone(),
                    base.clone(),
                    token.clone(),
                    running.clone(),
                    requests.clone(),
                );
                watchers.push(tokio::spawn(async move {
                    while running.load(Ordering::Relaxed) {
                        let suffix = match worker % 3 {
                            0 => "maintenance/health?refresh=true",
                            1 => "maintenance/pools/health?deep_scan=false",
                            _ => "capacity/fragment",
                        };
                        let value = json_response(
                            client
                                .get(format!("{base}/{suffix}"))
                                .bearer_auth(&token)
                                .send()
                                .await
                                .unwrap(),
                            200,
                        )
                        .await;
                        if worker % 3 == 0 {
                            assert_eq!(value["monitoring"]["automatic_content_scan"], false);
                        }
                        requests.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }));
            }
        }
        let path = format!("bench/run-{run}.bin");
        let route = format!("{base}/upload/multipart/durable");
        let start = std::time::Instant::now();
        let upload=json_response(client.post(format!("{route}/init/{path}?durability=strict")).bearer_auth(&token).json(&json!({"total_size_bytes":part.len()as u64*parts,"part_size_bytes":part.len(),"expected_parts":parts})).send().await.unwrap(),200).await;
        let version = upload["version_id"].as_str().unwrap();
        let session = upload["session_id"].as_str().unwrap();
        let mut latencies = Vec::new();
        for index in 0..parts {
            let sent = std::time::Instant::now();
            json_response(
                client
                    .put(format!("{route}/{version}/{index}"))
                    .bearer_auth(&token)
                    .body(part.clone())
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await;
            latencies.push(sent.elapsed().as_secs_f64() * 1000.);
        }
        json_response(
            client
                .post(format!("{route}/{version}/{session}/finalize"))
                .bearer_auth(&token)
                .json(&json!({}))
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        let upload_seconds = start.elapsed().as_secs_f64();
        let start = std::time::Instant::now();
        let mut response = client
            .get(format!("{base}/objects/get/{path}"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let mut received = 0u64;
        let mut digest = Sha256::new();
        while let Some(chunk) = response.chunk().await.unwrap() {
            received += chunk.len() as u64;
            digest.update(chunk);
        }
        let download_seconds = start.elapsed().as_secs_f64();
        assert_eq!(received, part.len() as u64 * parts);
        assert_eq!(digest.finalize(), expected);
        running.store(false, Ordering::Relaxed);
        for task in watchers {
            task.await.unwrap();
        }
        let end = json_response(
            client
                .get(format!("{base}/maintenance/health?live_counters=true"))
                .bearer_auth(&token)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        latencies.sort_by(f64::total_cmp);
        results.push(json!({"run":run,"monitoring":monitoring,"bytes":received,"upload_seconds":upload_seconds,"download_seconds":download_seconds,"upload_mib_s":256./upload_seconds,"download_mib_s":256./download_seconds,"monitoring_requests":requests.load(Ordering::Relaxed),"part_p50_ms":latencies[15],"part_p95_ms":latencies[30],"sha256_verified":true,"metadata_sample_sequence_before":warm["components"]["metadata"]["observation"]["sequence"],"metadata_sample_sequence_after":end["components"]["metadata"]["observation"]["sequence"]}));
    }
    let report = json!({"runs":results,"part_bytes":part.len(),"concurrent_monitor_clients":4,"monitor_interval_seconds":30,"order":"A B B A A B","cache_dropped":false,"cold_disk_claim":false});
    std::fs::write(
        stack.root.join("MONITORING-OVERHEAD.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    eprintln!("{}", serde_json::to_string_pretty(&report).unwrap());
}
