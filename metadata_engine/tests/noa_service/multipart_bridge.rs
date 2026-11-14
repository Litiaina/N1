//! External part boundaries remain independent of LINK stripes. A small memory
//! budget must preserve durable retries, corruption refusal and restart recovery.
use super::*;
use std::io::{Seek, SeekFrom, Write};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real N1/M1 processes; interrupt bridge conversion and verify every durable client ACK"]
async fn bridged_finalize_preserves_external_acks_after_object_and_both_process_crashes() {
    use m1::object_store::{NativeObjectStore, ObjectStoreConfig};
    let mut stack = Stack::with_block_bytes(262144).await;
    drop(stack.metadata.take());
    let config = stack.root.join("m1_config.ini");
    let mut ini = ini::Ini::load_from_file(&config).unwrap();
    ini.with_section(Some("upload_config"))
        .set("max_part_size", "2097152")
        .set("max_concurrent_io", "2")
        .set("upload_memory_budget_bytes", "1048576");
    ini.write_to_file(&config).unwrap();
    stack.start_metadata().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (alias, token) = reliability::fragment(&client, &base, "Bridge conversion crashes").await;
    let credentials: Value = serde_json::from_slice(
        &std::fs::read(stack.object_config.with_extension("credentials.json")).unwrap(),
    )
    .unwrap();
    let route = format!("{base}/upload/multipart/durable");
    let bytes: Vec<u8> = (0..16 * 1024 * 1024 + 127)
        .map(|i| (i * 31 % 251) as u8)
        .collect();
    let part_bytes = 1_500_001;
    let count = bytes.len().div_ceil(part_bytes);
    for target in ["object", "both"] {
        let path = format!("bridge-{target}.bin");
        let upload = json_response(
            client
                .post(format!("{route}/init/{path}"))
                .bearer_auth(&token)
                .json(&json!({"total_size_bytes":bytes.len(),
                "expected_parts":count,"part_size_bytes":part_bytes}))
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        let version = upload["version_id"].as_str().unwrap();
        for (index, part) in bytes.chunks(part_bytes).enumerate().rev() {
            resume_progress::put(
                &client,
                &route,
                &token,
                version,
                index as u64,
                part.to_vec(),
            )
            .await;
        }
        // Strict client ACKs belong to durable M1 staging. N1's intermediate
        // copy needs its final publication barrier, not per-part flushes.
        assert_eq!(upload["durable"], true);
        drop(stack.metadata.take());
        let database = maintenance::db(&stack);
        let fragment = m1::fragments::resolve(&database, &alias).unwrap().id;
        let pending: Value = serde_json::from_slice(
            &database
                .snapshot()
                .get(
                    m1::Table::Uploads,
                    &m1::RecordKey::new(fragment, version).unwrap(),
                )
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(pending["upload"]["durable"], false);
        let key = pending["upload"]["object_key"].as_str().unwrap().to_owned();
        drop(database);
        stack.start_metadata().await;
        let finalize = format!(
            "{route}/{version}/{}/finalize",
            upload["session_id"].as_str().unwrap()
        );
        let objects = NativeObjectStore::new(ObjectStoreConfig {
            native_url: credentials["native_url"].as_str().unwrap().into(),
            fragment: credentials["fragment"].as_str().unwrap().into(),
            secret: credentials["secret"].as_str().unwrap().into(),
            max_connections: 1,
            tls: None,
        })
        .unwrap();
        let request = client.post(&finalize).bearer_auth(&token).json(&json!({}));
        let finalizing = tokio::spawn(async move { request.send().await });
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let health = objects
                .control(n1_link::noa::Command::Health)
                .await
                .unwrap();
            if health["operations"]["active_operations"].as_u64().unwrap() >= 1 {
                break;
            }
            assert!(
                !finalizing.is_finished(),
                "bridge finalized before conversion could be interrupted"
            );
            assert!(
                tokio::time::Instant::now() < deadline,
                "bridge did not begin native conversion"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // The event is observed in N1, not inferred from a fixed sleep.
        drop(stack.object.take());
        if target == "both" {
            drop(stack.metadata.take());
        }
        let interrupted = finalizing.await.unwrap();
        assert!(interrupted.is_err() || !interrupted.unwrap().status().is_success());
        // Lose one intermediate native payload as well as volatile completion.
        // M1 must reconstruct it from its durable source without changing LINK.
        let settings: Value =
            serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
        let layout: n1_link::PoolLayout =
            serde_json::from_value(settings["pools"][0]["layout"].clone()).unwrap();
        let roots: Vec<PathBuf> =
            serde_json::from_value(settings["pools"][0]["roots"].clone()).unwrap();
        let fp = n1_link::ObjectKey::inspect(&key).unwrap().fingerprint();
        let staged = roots[layout.member_slot(fp, 0).unwrap()].join(fp.staging_path());
        std::fs::OpenOptions::new()
            .write(true)
            .open(&staged)
            .unwrap()
            .set_len(128)
            .unwrap();
        stack.start_object().await;
        if stack.metadata.is_none() {
            stack.start_metadata().await;
        }
        let status = resume_progress::status(&client, &route, &token, version).await;
        assert_eq!(
            status["uploaded_parts"],
            json!((0..count).collect::<Vec<_>>())
        );
        assert_eq!(status["completed"], false);
        assert_eq!(
            client
                .get(format!("{base}/objects/get/{path}"))
                .bearer_auth(&token)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        for _ in 0..2 {
            let result = json_response(
                client
                    .post(&finalize)
                    .bearer_auth(&token)
                    .json(&json!({}))
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await;
            assert_eq!(result["version_id"], version);
        }
        let response = client
            .get(format!("{base}/objects/get/{path}"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["etag"]
                .to_str()
                .unwrap()
                .trim_matches('"'),
            version
        );
        assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
    }
}

pub(super) fn temporary_parts(staging: &Path, version: &str) -> Vec<PathBuf> {
    std::fs::read_dir(staging)
        .unwrap()
        .flat_map(|entry| {
            std::fs::read_dir(entry.unwrap().path().join(version))
                .into_iter()
                .flatten()
        })
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "tmp"))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real private N1/M1 processes and isolated filesystem fixtures required"]
async fn bounded_bridge_preserves_retries_restart_and_prepublication_checksum_validation() {
    let mut stack = Stack::with_block_bytes(262144).await;
    drop(stack.metadata.take());
    let config = stack.root.join("m1_config.ini");
    let mut ini = ini::Ini::load_from_file(&config).unwrap();
    ini.with_section(Some("upload_config"))
        .set("upload_memory_budget_bytes", "1048576")
        // Exercise several native writes crossing external part boundaries.
        .set("max_part_size", "524288");
    ini.write_to_file(&config).unwrap();
    stack.start_metadata().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token) = reliability::fragment(&client, &base, "bounded bridge").await;
    let bytes: Vec<u8> = (0..2 * 1024 * 1024 + 123)
        .map(|i| (i * 31 % 251) as u8)
        .collect();
    let part_bytes = 150001;
    let count = bytes.len().div_ceil(part_bytes);
    let route = format!("{base}/upload/multipart/durable");
    let upload = json_response(
        client
            .post(format!("{route}/init/bridge.bin?durability=strict"))
            .bearer_auth(&token)
            .json(&json!({
                "total_size_bytes": bytes.len(),
                "expected_parts": count,
                "part_size_bytes": part_bytes,
            }))
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let version = upload["version_id"].as_str().unwrap();
    let finalize = format!(
        "{route}/{version}/{}/finalize",
        upload["session_id"].as_str().unwrap()
    );
    let fragment = |index: usize| {
        bytes[index * part_bytes..bytes.len().min((index + 1) * part_bytes)].to_vec()
    };
    let settings = m1::server::ServerConfig::load(&config).unwrap();
    // Two sources reserve the minimum supported 1 MiB budget. A third part
    // must receive a bounded 503 instead of a slot/memory deadlock.
    let url = reqwest::Url::parse(&stack.url).unwrap();
    let mut interrupted = Vec::new();
    for index in [1, 3] {
        let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", url.port().unwrap()))
            .await
            .unwrap();
        let request = format!(
            "PUT /noa/v1/upload/multipart/durable/{version}/{index}?durability=strict HTTP/1.1\r\n\
             Host: localhost\r\n\
             Authorization: Bearer {token}\r\n\
             Content-Length: {part_bytes}\r\n\
             Connection: close\r\n\r\n"
        );
        socket.write_all(request.as_bytes()).await.unwrap();
        socket.write_all(&fragment(index)[..4096]).await.unwrap();
        interrupted.push(socket);
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while temporary_parts(&settings.staging_dir, version).len() != 2 {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    json_response(
        client
            .put(format!("{route}/{version}/2?durability=strict"))
            .bearer_auth(&token)
            .body(fragment(2))
            .send()
            .await
            .unwrap(),
        503,
    )
    .await;
    drop(interrupted);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !temporary_parts(&settings.staging_dir, version).is_empty() {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let state = json_response(
        client
            .get(format!("{route}/{version}"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(state["uploaded_parts"], json!([]));
    for index in [2, 0, count - 1] {
        resume_progress::put(
            &client,
            &route,
            &token,
            version,
            index as u64,
            fragment(index),
        )
        .await;
    }
    resume_progress::put(&client, &route, &token, version, 2, fragment(2)).await;
    let mut conflict = fragment(2);
    conflict[0] ^= 1;
    json_response(
        client
            .put(format!("{route}/{version}/2?durability=strict"))
            .bearer_auth(&token)
            .body(conflict)
            .send()
            .await
            .unwrap(),
        409,
    )
    .await;
    drop(stack.metadata.take());
    drop(stack.object.take());
    stack.start_object().await;
    stack.start_metadata().await;
    let state = json_response(
        client
            .get(format!("{route}/{version}"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(state["uploaded_parts"], json!([0, 2, count - 1]));
    for index in 0..count {
        if ![0, 2, count - 1].contains(&index) {
            resume_progress::put(
                &client,
                &route,
                &token,
                version,
                index as u64,
                fragment(index),
            )
            .await;
        }
    }
    drop(stack.metadata.take());
    let config = m1::server::ServerConfig::load(&config).unwrap();
    let staged: Vec<_> = std::fs::read_dir(&config.staging_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path().join(version).join("2.part"))
        .filter(|path| path.is_file())
        .collect();
    assert_eq!(staged.len(), 1);
    let staged = staged.into_iter().next().unwrap();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&staged)
        .unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&[fragment(2)[0] ^ 1]).unwrap();
    file.sync_all().unwrap();
    drop(file);
    stack.start_metadata().await;
    json_response(
        client
            .post(&finalize)
            .bearer_auth(&token)
            .json(&json!({}))
            .send()
            .await
            .unwrap(),
        422,
    )
    .await;
    assert_eq!(
        client
            .get(format!("{base}/objects/get/bridge.bin"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    resume_progress::put(&client, &route, &token, version, 2, fragment(2)).await;
    // Corruption late in the source must preserve the already converted,
    // verified native prefix. Retrying the original bytes must finish with
    // the same identity; corrupt bytes must never poison a native frame.
    drop(stack.metadata.take());
    let tail_path = staged.parent().unwrap().join(format!("{}.part", count - 1));
    let mut tail = std::fs::OpenOptions::new()
        .write(true)
        .open(&tail_path)
        .unwrap();
    tail.write_all(&[fragment(count - 1)[0] ^ 1]).unwrap();
    tail.sync_all().unwrap();
    drop(tail);
    stack.start_metadata().await;
    json_response(
        client
            .post(&finalize)
            .bearer_auth(&token)
            .json(&json!({}))
            .send()
            .await
            .unwrap(),
        422,
    )
    .await;
    assert_eq!(
        client
            .get(format!("{base}/objects/get/bridge.bin"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    resume_progress::put(
        &client,
        &route,
        &token,
        version,
        (count - 1) as u64,
        fragment(count - 1),
    )
    .await;
    for _ in 0..2 {
        let published = json_response(
            client
                .post(&finalize)
                .bearer_auth(&token)
                .json(&json!({}))
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        assert_eq!(published["version_id"], version);
    }
    let response = client
        .get(format!("{base}/objects/get/bridge.bin"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["etag"]
            .to_str()
            .unwrap()
            .trim_matches('"'),
        version
    );
    assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
    assert!(!staged.exists());
}
