//! Isolate monitoring from differing upload placement and post-write cache state.
use super::*;

// Restrict advice to the new fixture's own immutable shards. This never drops
// global caches or touches another campaign; device/controller caches remain.
fn evict_fixture_shards(settings: &Value) -> usize {
    use std::os::fd::AsRawFd;
    let mut pending: Vec<PathBuf> = settings["pools"][0]["roots"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| PathBuf::from(p.as_str().unwrap()))
        .collect();
    let mut count = 0;
    while let Some(path) = pending.pop() {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                pending.push(entry.path());
            } else if entry.file_type().unwrap().is_file()
                && entry
                    .path()
                    .extension()
                    .is_some_and(|suffix| suffix == "shard")
            {
                let file = std::fs::File::open(entry.path()).unwrap();
                assert_eq!(
                    unsafe {
                        libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED)
                    },
                    0
                );
                count += 1;
            }
        }
    }
    assert_eq!(count, 2, "expected exactly one immutable two-member object");
    count
}

async fn verified_read(client: &Client, url: &str, token: &str, version: &str, hash: &[u8]) {
    let mut response = client.get(url).bearer_auth(token).send().await.unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["etag"], format!("\"{version}\""));
    let mut received = 0u64;
    let mut digest = Sha256::new();
    while let Some(chunk) = response.chunk().await.unwrap() {
        received += chunk.len() as u64;
        digest.update(chunk);
    }
    assert_eq!(received, 256 << 20);
    assert_eq!(&digest.finalize()[..], hash);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "same immutable 256 MiB object, eight monitoring on/off phases, explicit private two-HDD roots"]
async fn same_object_read_replay_measures_monitoring_without_upload_or_placement_changes() {
    use std::os::unix::fs::MetadataExt;
    let first = PathBuf::from(std::env::var_os("N1_OBJECT_TEST_FIRST_ROOT").unwrap());
    let second = PathBuf::from(std::env::var_os("N1_OBJECT_TEST_SECOND_ROOT").unwrap());
    assert_ne!(
        first.metadata().unwrap().dev(),
        second.metadata().unwrap().dev()
    );
    let mut stack = Stack::with_block_bytes(262144).await;
    drop(stack.metadata.take());
    drop(stack.object.take());
    let mut settings: Value =
        serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
    settings["runtime"]["monitor_interval_seconds"] = json!(30);
    std::fs::write(&stack.object_config, serde_json::to_vec(&settings).unwrap()).unwrap();
    let path = stack.root.join("m1_config.ini");
    let mut ini = ini::Ini::load_from_file(&path).unwrap();
    ini.with_section(Some("upload_config"))
        .set("monitor_interval_seconds", "30");
    ini.write_to_file(path).unwrap();
    stack.start_object().await;
    stack.start_metadata().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(300))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token, _) = maintenance::auth(&client, &base, "monitoring-read-replay").await;
    let part: Vec<_> = (0..8 * 1024 * 1024).map(|n| (n % 251) as u8).collect();
    let mut hash = Sha256::new();
    for _ in 0..32 {
        hash.update(&part);
    }
    let expected = hash.finalize();
    let route = format!("{base}/upload/multipart/durable");
    let upload = json_response(
        client
            .post(format!("{route}/init/same.bin?durability=strict"))
            .bearer_auth(&token)
            .json(&json!({"total_size_bytes":256u64<<20,
            "part_size_bytes":8<<20,"expected_parts":32}))
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let version = upload["version_id"].as_str().unwrap();
    for index in 0..32 {
        resume_progress::put(&client, &route, &token, version, index, part.clone()).await;
    }
    let completed = json_response(
        client
            .post(format!(
                "{route}/{version}/{}/finalize",
                upload["session_id"].as_str().unwrap()
            ))
            .bearer_auth(&token)
            .json(&json!({}))
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(completed["version_id"], version);
    let url = format!("{base}/objects/get/same.bin");
    verified_read(&client, &url, &token, version, &expected).await;
    let pids = [
        stack.metadata.as_ref().unwrap().0.id(),
        stack.object.as_ref().unwrap().0.id(),
    ];
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    assert!(hz > 0);
    let evict = std::env::var("N1_MONITOR_READ_EVICT_FILE_CACHE").is_ok_and(|v| v == "1");
    let mut runs = Vec::new();
    for monitoring in [false, true, true, false, true, false, false, true] {
        let advised_shards = if evict {
            evict_fixture_shards(&settings)
        } else {
            0
        };
        let running = Arc::new(AtomicBool::new(true));
        let mut watchers = Vec::new();
        if monitoring {
            for worker in 0..4 {
                let (client, base, token, running) =
                    (client.clone(), base.clone(), token.clone(), running.clone());
                watchers.push(tokio::spawn(async move {
                    let mut latencies = Vec::new();
                    while running.load(Ordering::Relaxed) {
                        let route = match worker % 3 {
                            0 => "maintenance/health?refresh=true",
                            1 => "maintenance/pools/health?deep_scan=false",
                            _ => "capacity/fragment",
                        };
                        let start = std::time::Instant::now();
                        json_response(
                            client
                                .get(format!("{base}/{route}"))
                                .bearer_auth(&token)
                                .send()
                                .await
                                .unwrap(),
                            200,
                        )
                        .await;
                        latencies.push(start.elapsed().as_secs_f64() * 1000.0);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    latencies
                }));
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        let before = pids.map(process_sample);
        let start = std::time::Instant::now();
        verified_read(&client, &url, &token, version, &expected).await;
        let seconds = start.elapsed().as_secs_f64();
        let after = pids.map(process_sample);
        running.store(false, Ordering::Relaxed);
        let mut latencies = Vec::new();
        for watcher in watchers {
            latencies.extend(watcher.await.unwrap());
        }
        for i in 0..2 {
            assert_eq!(before[i].start, after[i].start);
        }
        runs.push(
            json!({"monitoring":monitoring,"seconds":seconds,"mib_s":256.0/seconds,
            "shards_advised_dontneed":advised_shards,
            "metadata_cpu_seconds":(after[0].ticks-before[0].ticks) as f64/hz as f64,
            "object_cpu_seconds":(after[1].ticks-before[1].ticks) as f64/hz as f64,
            "metadata_rss_after_bytes":after[0].rss,"object_rss_after_bytes":after[1].rss,
            "monitoring_request_ms":latencies,"sha256_verified":true}),
        );
    }
    let report = json!({"version":version,"bytes":256u64<<20,"same_object":true,
        "warmup_full_reads":1,"global_cache_dropped":false,
        "fixture_file_cache_eviction_requested":evict,"order":"A B B A B A A B",
        "monitor_clients":4,"monitor_poll_millis":100,"runs":runs});
    std::fs::write(
        stack.root.join("MONITORING-READ-REPLAY.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    eprintln!("{}", serde_json::to_string(&report).unwrap());
}
