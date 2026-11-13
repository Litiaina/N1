//! Real ENOSPC on a private, size-limited filesystem. The independent ACK
//! oracle remains on healthy storage; neither a mock nor the host disk is filled.
use super::*;
use std::{fs, io::Write, os::unix::fs::MetadataExt};

enum Target {
    Object,
    Metadata,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a private bounded filesystem for staging and healthy external ACK storage"]
async fn staging_disk_full_preserves_durable_parts_and_resumes_the_same_identity() {
    let pressure_root = PathBuf::from(std::env::var_os("N1_RESOURCE_PRESSURE_ROOT").unwrap());
    bounded_filesystem(&pressure_root);
    let evidence = PathBuf::from(std::env::var_os("N1_RESOURCE_EVIDENCE_ROOT").unwrap())
        .join(format!("staging-full-{}", ulid::Ulid::new()));
    fs::create_dir(&evidence).unwrap();
    assert_ne!(
        fs::metadata(&pressure_root).unwrap().dev(),
        fs::metadata(&evidence).unwrap().dev()
    );
    let mut stack = Stack::with_block_bytes(262144).await;
    drop(stack.metadata.take());
    let config = stack.root.join("m1_config.ini");
    let staging = pressure_root.join(format!("staging-{}", ulid::Ulid::new()));
    let mut ini = ini::Ini::load_from_file(&config).unwrap();
    ini.with_section(Some("metadata_engine"))
        .set("staging_dir", staging.display().to_string());
    ini.write_to_file(&config).unwrap();
    stack.start_metadata().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token) = reliability::fragment(&client, &base, "staging ENOSPC").await;
    let bytes: Vec<_> = (0..900003).map(|i| (i * 31 % 251) as u8).collect();
    let seed = maintenance::put(&client, &base, &token, "seed.bin", &bytes[..65537]).await;
    let route = format!("{base}/upload/multipart/durable");
    let hint = json!({"total_size_bytes":bytes.len(),"part_size_bytes":300001,"expected_parts":3});
    let init_url = format!("{route}/init/staged.bin");
    let upload = json_response(
        client
            .post(&init_url)
            .bearer_auth(&token)
            .json(&hint)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let version = upload["version_id"].as_str().unwrap();
    resume_progress::put(
        &client,
        &route,
        &token,
        version,
        0,
        bytes[..300001].to_vec(),
    )
    .await;
    reliability::record(
        &evidence.join("prior-acks.json"),
        &json!({"seed":seed,"upload":upload,"uploaded_parts":[0]}),
    );
    let (pressure, pressure_bytes) = fill(&pressure_root);
    let started = std::time::Instant::now();
    let failed = client
        .put(format!("{route}/{version}/1"))
        .bearer_auth(&token)
        .body(bytes[300001..600002].to_vec())
        .send()
        .await
        .unwrap();
    let status = failed.status().as_u16();
    reliability::record(
        &evidence.join("failed-part.json"),
        &json!({"status":status,
        "seconds":started.elapsed().as_secs_f64(),"response":failed.json::<Value>().await.unwrap()}),
    );
    assert_eq!(status, 507, "staging ENOSPC returned {status}");
    drop(stack.metadata.take());
    drop(stack.object.take());
    drop(pressure);
    stack.start_object().await;
    stack.start_metadata().await;
    let reused = json_response(
        client
            .post(&init_url)
            .bearer_auth(&token)
            .json(&hint)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(reused, upload);
    assert_eq!(
        resume_progress::status(&client, &route, &token, version).await["uploaded_parts"],
        json!([0])
    );
    assert!(
        read(
            &client,
            &base,
            &token,
            "seed.bin",
            &bytes[..65537],
            Some(&seed)
        )
        .await
    );
    for i in 1..3 {
        resume_progress::put(
            &client,
            &route,
            &token,
            version,
            i as u64,
            bytes[i * 300001..(i + 1) * 300001].to_vec(),
        )
        .await;
    }
    let finish = format!(
        "{route}/{version}/{}/finalize",
        upload["session_id"].as_str().unwrap()
    );
    for _ in 0..2 {
        let done = json_response(
            client
                .post(&finish)
                .bearer_auth(&token)
                .json(&json!({}))
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        assert_eq!(done["version_id"], version);
    }
    assert!(read(&client, &base, &token, "staged.bin", &bytes, Some(version)).await);
    drop(stack.metadata.take());
    let db = maintenance::db(&stack);
    let checked = db.verify().unwrap();
    reliability::record(
        &evidence.join("PASSED.json"),
        &json!({"passed":true,
        "pressure_bytes":pressure_bytes,"prior_part_acks":1,"same_identity":true,
        "failed_part_status":status,"offline_records_checked":checked}),
    );
}

struct Pressure(PathBuf);
impl Drop for Pressure {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
        if let Some(parent) = self.0.parent()
            && let Ok(file) = fs::File::open(parent)
        {
            let _ = file.sync_all();
        }
    }
}

fn bounded_filesystem(path: &Path) -> u64 {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};
    let path = CString::new(path.as_os_str().as_bytes()).unwrap();
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    assert_eq!(
        unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) },
        0
    );
    let stat = unsafe { stat.assume_init() };
    let bytes = stat.f_blocks * stat.f_frsize;
    assert!(
        (16 << 20..=1 << 30).contains(&bytes),
        "refusing to fill a filesystem outside the private 16 MiB–1 GiB range"
    );
    bytes
}

fn fill(path: &Path) -> (Pressure, u64) {
    use std::os::unix::fs::OpenOptionsExt;
    let capacity = bounded_filesystem(path);
    let path = path.join(format!("pressure-{}.bin", ulid::Ulid::new()));
    let guard = Pressure(path.clone());
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .unwrap();
    let mut written = 0;
    // A rejected 1 MiB allocation can leave enough blocks for a small part.
    // Exhaust the remaining allocation units too, rather than treating that
    // first large-write ENOSPC as proof that a 300 KiB part cannot fit.
    for block_bytes in [1 << 20, 4096] {
        let block = vec![0x57; block_bytes];
        loop {
            match file.write(&block) {
                Ok(0) => panic!("zero-length pressure write"),
                Ok(bytes) => {
                    written += bytes as u64;
                    assert!(written <= capacity, "filesystem did not enforce its size");
                }
                Err(error) if error.raw_os_error() == Some(libc::ENOSPC) => break,
                Err(error) => panic!("pressure must reach actual ENOSPC: {error}"),
            }
        }
    }
    if let Err(error) = file.sync_all() {
        assert_eq!(error.raw_os_error(), Some(libc::ENOSPC));
    }
    (guard, written)
}

async fn read(
    client: &Client,
    base: &str,
    token: &str,
    path: &str,
    bytes: &[u8],
    version: Option<&str>,
) -> bool {
    let response = client
        .get(format!("{base}/objects/get/{path}"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    if response.status() == StatusCode::NOT_FOUND && version.is_none() {
        return false;
    }
    assert_eq!(response.status(), StatusCode::OK);
    let actual = response.headers().get("etag").unwrap().to_str().unwrap();
    assert!(!actual.trim_matches('"').is_empty());
    if let Some(version) = version {
        assert_eq!(actual.trim_matches('"'), version);
    }
    assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
    true
}

async fn exercise(target: Target) {
    let pressure = PathBuf::from(
        std::env::var_os("N1_RESOURCE_PRESSURE_ROOT").expect("private bounded filesystem required"),
    );
    bounded_filesystem(&pressure);
    let evidence = PathBuf::from(
        std::env::var_os("N1_RESOURCE_EVIDENCE_ROOT")
            .expect("healthy external ACK directory required"),
    );
    assert_ne!(
        fs::metadata(&pressure).unwrap().dev(),
        fs::metadata(&evidence).unwrap().dev()
    );
    let mut stack = Stack::with_block_bytes(262144).await;
    let config = m1::server::ServerConfig::load(&stack.root.join("m1_config.ini")).unwrap();
    let affected = match target {
        Target::Metadata => config.data_dir,
        Target::Object => PathBuf::from(
            std::env::var_os("N1_OBJECT_TEST_FIRST_ROOT").expect("explicit object member required"),
        ),
    };
    assert_eq!(
        fs::metadata(&affected).unwrap().dev(),
        fs::metadata(&pressure).unwrap().dev()
    );
    let evidence = evidence.join(format!("resource-{}", ulid::Ulid::new()));
    fs::create_dir(&evidence).unwrap();
    let client = Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token) = reliability::fragment(&client, &base, "ENOSPC witness").await;
    let bytes = vec![0x73; 65537];
    let seed = maintenance::put(&client, &base, &token, "seed.bin", &bytes).await;
    reliability::record(
        &evidence.join("seed-ack.json"),
        &json!({"path":"seed.bin","version":seed}),
    );
    let (pressure, pressure_bytes) = fill(&pressure);
    let mut acknowledged = vec![("seed.bin".to_owned(), seed)];
    let mut attempts = Vec::new();
    let mut rejected = false;
    for index in 0..64 {
        let path = format!("pressure-{index:02}.bin");
        let started = std::time::Instant::now();
        let response = client
            .post(format!("{base}/upload/one-shot/{path}?durability=strict"))
            .bearer_auth(&token)
            .body(bytes.clone())
            .send()
            .await
            .expect("ENOSPC must produce a bounded response rather than a timeout");
        let status = response.status();
        let body: Value = response.json().await.unwrap();
        let row = json!({"path":path,"status":status.as_u16(),"response":body,"seconds":started.elapsed().as_secs_f64()});
        reliability::record(&evidence.join(format!("attempt-{index:02}.json")), &row);
        attempts.push(path.clone());
        if status == StatusCode::CREATED {
            acknowledged.push((path, body["version_id"].as_str().unwrap().to_owned()));
        } else {
            match target {
                Target::Object => assert_eq!(status, StatusCode::INSUFFICIENT_STORAGE),
                Target::Metadata => assert!(matches!(status.as_u16(), 500 | 503 | 507)),
            }
            rejected = true;
            break;
        }
    }
    assert!(
        rejected,
        "no storage failure after 64 writes on a full filesystem"
    );
    reliability::record(
        &evidence.join("acks-before-crash.json"),
        &json!(acknowledged),
    );
    drop(stack.metadata.take());
    drop(stack.object.take());
    drop(pressure);
    stack.start_object().await;
    stack.start_metadata().await;
    for (path, version) in &acknowledged {
        assert!(read(&client, &base, &token, path, &bytes, Some(version)).await);
    }
    // An unacknowledged operation can recover wholly present or absent. It
    // must never expose an incomplete payload or invalidate an earlier ACK.
    for path in &attempts {
        read(&client, &base, &token, path, &bytes, None).await;
    }
    let recovered =
        maintenance::put(&client, &base, &token, "after-space-restored.bin", &bytes).await;
    assert!(
        read(
            &client,
            &base,
            &token,
            "after-space-restored.bin",
            &bytes,
            Some(&recovered)
        )
        .await
    );
    drop(stack.metadata.take());
    let database = maintenance::db(&stack);
    let verified = database.verify().unwrap();
    reliability::record(
        &evidence.join("PASSED.json"),
        &json!({
            "passed":true,"fault":"actual ENOSPC followed by SIGKILL of both processes",
            "pressure_bytes":pressure_bytes,"prior_acknowledgements_verified":acknowledged.len(),
            "attempts_checked":attempts.len(),"offline_records_verified":verified,
            "new_write_after_recovery_verified":true,"fixture":stack.root
        }),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a private bounded filesystem for the object member and healthy external ACK storage"]
async fn object_disk_full_preserves_acks_and_refuses_incomplete_publication() {
    exercise(Target::Object).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a private bounded filesystem for M1 and healthy external ACK/object storage"]
async fn metadata_disk_full_preserves_acks_and_recovers_after_space_is_restored() {
    exercise(Target::Metadata).await;
}
