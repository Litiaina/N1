//! Alternate compatible M1 binaries over one database and one running N1.
//! This separates the export-worker cost from fresh-HDD-fixture variation.
use super::*;
use preparation_bench::{distribution, resources};
use sha2::{Digest, Sha256};
use std::time::Instant;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit frozen M1 baseline/candidate and performance output required"]
async fn retained_export_scheduler_same_database_performance() {
    let baseline = PathBuf::from(std::env::var_os("M1_BASELINE_TEST_BINARY").unwrap());
    let candidate = PathBuf::from(std::env::var_os("M1_CANDIDATE_TEST_BINARY").unwrap());
    let output = PathBuf::from(std::env::var_os("N1_EXPORT_PERF_OUTPUT").unwrap());
    assert!(!output.exists(), "never overwrite a measurement");
    let binary_hashes = [baseline.as_path(), candidate.as_path()]
        .map(|path| hex::encode(Sha256::digest(std::fs::read(path).unwrap())));
    let mut stack = Stack::with_block_bytes(262144).await;
    drop(stack.metadata.take());
    let path = stack.root.join("m1_config.ini");
    let mut ini = ini::Ini::load_from_file(&path).unwrap();
    ini.with_section(Some("rate_limiter_config"))
        .set("enable_rate_limiting", "true");
    for class in ["auth", "global", "view", "upload"] {
        ini.with_section(Some("rate_limiter_config"))
            .set(format!("{class}_limit_per_second"), "10000")
            .set(format!("{class}_limit_burst_size"), "10000");
    }
    ini.write_to_file(&path).unwrap();
    let client = Client::builder()
        .timeout(Duration::from_secs(300))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    stack.start_metadata_binary(&baseline, &[]).await;
    let (_, token, _) = maintenance::auth(&client, &base, "same-database-export").await;
    let bytes = vec![71u8; 4096];
    json_response(
        client
            .post(format!(
                "{base}/upload/one-shot/probe.bin?durability=strict"
            ))
            .bearer_auth(&token)
            .body(bytes.clone())
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
    let object_pid = stack.object.as_ref().unwrap().0.id();
    let mut phases = Vec::new();
    // ABBA followed by BAAB balances growth, ordering and warmup effects.
    for (phase, use_candidate) in [false, true, true, false, true, false, false, true]
        .into_iter()
        .enumerate()
    {
        drop(stack.metadata.take());
        let binary = if use_candidate { &candidate } else { &baseline };
        stack.start_metadata_binary(binary, &[]).await;
        let probe = client
            .get(format!("{base}/objects/get/probe.bin"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(probe.status(), 200);
        assert_eq!(probe.bytes().await.unwrap().as_ref(), bytes);
        assert_eq!(stack.object.as_ref().unwrap().0.id(), object_pid);
        let metadata_before = resources(stack.metadata.as_ref().unwrap().0.id());
        let object_before = resources(object_pid);
        let mut uploads = Vec::new();
        let mut reads = Vec::new();
        let mut renames = Vec::new();
        let mut trash = Vec::new();
        for index in 0..100 {
            let key = format!("phase-{phase:02}/small-{index:03}.bin");
            let renamed = format!("phase-{phase:02}/renamed-{index:03}.bin");
            let now = Instant::now();
            json_response(
                client
                    .post(format!("{base}/upload/one-shot/{key}?durability=strict"))
                    .bearer_auth(&token)
                    .body(bytes.clone())
                    .send()
                    .await
                    .unwrap(),
                201,
            )
            .await;
            uploads.push(now.elapsed().as_secs_f64() * 1000.0);
            let now = Instant::now();
            let response = client
                .get(format!("{base}/objects/get/{key}"))
                .bearer_auth(&token)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
            reads.push(now.elapsed().as_secs_f64() * 1000.0);
            let now = Instant::now();
            json_response(
                client
                    .post(format!("{base}/objects/rename"))
                    .bearer_auth(&token)
                    .json(&json!({"source_key":key,"destination_key":renamed}))
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await;
            renames.push(now.elapsed().as_secs_f64() * 1000.0);
            let now = Instant::now();
            json_response(
                client
                    .delete(format!("{base}/objects/{renamed}"))
                    .bearer_auth(&token)
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await;
            trash.push(now.elapsed().as_secs_f64() * 1000.0);
        }
        phases.push(json!({"phase":phase,"candidate":use_candidate,
            "small_upload":distribution(uploads),"small_get":distribution(reads),
            "rename":distribution(renames),"trash":distribution(trash),
            "metadata_before":metadata_before,"metadata_after":resources(stack.metadata.as_ref().unwrap().0.id()),
            "object_before":object_before,"object_after":resources(object_pid)}));
    }
    let result = json!({"fixture":stack.root,"same_database":true,"same_running_N1_pid":object_pid,
        "baseline_sha256":binary_hashes[0],"candidate_sha256":binary_hashes[1],
        "small_objects_per_phase":100,"small_bytes":4096,"strict_durability":true,
        "quota_per_second_and_burst":10000,"phases":phases});
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)
        .unwrap();
    use std::io::Write;
    file.write_all(&serde_json::to_vec_pretty(&result).unwrap())
        .unwrap();
    file.sync_all().unwrap();
}
