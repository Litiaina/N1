//! Real size-policy checks, including streamed bodies and allocation-free
//! large-object admission. Explicit opt-in roots preserve the HDD test boundary.
use super::*;

fn streamed(bytes: &[u8]) -> reqwest::Body {
    let chunks = bytes
        .chunks(32768)
        .map(|part| Ok::<_, std::io::Error>(part.to_vec()))
        .collect::<Vec<_>>();
    reqwest::Body::wrap_stream(futures_util::stream::iter(chunks))
}

async fn transfers(client: &Client, base: &str, token: &str, prefix: &str) {
    let bytes = (0..2 * 1024 * 1024 + 7)
        .map(|n| (n % 251) as u8)
        .collect::<Vec<_>>();
    for unknown in [false, true] {
        let path = format!("{prefix}/stream-{unknown}.bin");
        let body = if unknown {
            streamed(&bytes)
        } else {
            bytes.clone().into()
        };
        json_response(
            client
                .post(format!("{base}/upload/one-shot/{path}"))
                .bearer_auth(token)
                .body(body)
                .send()
                .await
                .unwrap(),
            201,
        )
        .await;
        let response = client
            .get(format!("{base}/objects/get/{path}"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
        json_response(
            client
                .delete(format!("{base}/objects/{path}"))
                .bearer_auth(token)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
    }
}

async fn large_sessions(client: &Client, base: &str, token: &str, prefix: &str) {
    // These are admission/resume/cancel tests, not multi-terabyte payload tests.
    for durable in [false, true] {
        let route = if durable {
            "multipart/durable"
        } else {
            "multipart"
        };
        for (total, part) in [(15_u64 << 30, 8_u64 << 20), (1_u64 << 40, 32_u64 << 20)] {
            let shape = json!({"total_size_bytes":total,"part_size_bytes":part,"expected_parts":total.div_ceil(part)});
            let url = format!("{base}/upload/{route}/init/{prefix}/{durable}-{total}");
            let init = json_response(
                client
                    .post(&url)
                    .bearer_auth(token)
                    .json(&shape)
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await;
            let retry = json_response(
                client
                    .post(&url)
                    .bearer_auth(token)
                    .json(&shape)
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await;
            assert_eq!(init, retry);
            let version = init["version_id"].as_str().unwrap();
            let status_url = format!("{base}/upload/{route}/{version}");
            let status = json_response(
                client
                    .get(&status_url)
                    .bearer_auth(token)
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await;
            assert_eq!(status["uploaded_parts"], json!([]));
            json_response(
                client
                    .delete(&status_url)
                    .bearer_auth(token)
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await;
        }
        let response = client.post(format!("{base}/upload/{route}/init/{prefix}/oversized-part"))
            .bearer_auth(token).json(&json!({"total_size_bytes":64_u64<<20,"part_size_bytes":64_u64<<20,"expected_parts":1}))
            .send().await.unwrap();
        assert_eq!(response.status(), 413);
        let response = client.post(format!("{base}/upload/{route}/init/{prefix}/too-many-parts"))
            .bearer_auth(token).json(&json!({"total_size_bytes":131073_u64*4096,"part_size_bytes":4096,"expected_parts":131073}))
            .send().await.unwrap();
        assert_eq!(response.status(), 413);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit object binary and isolated storage roots"]
async fn finite_and_unlimited_policy_on_real_metadata_and_object_engines() {
    let mut stack = Stack::new().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let created = json_response(client.post(format!("{base}/fragments"))
        .json(&json!({"name":"Size policy witness","secret":"test-only-master","key":"test-only-fragment"})).send().await.unwrap(), 201).await;
    let logged = json_response(
        client
            .post(format!("{base}/auth/fragment"))
            .json(&json!({
                "fragment": created["fragment_hash"],
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
    let token = logged["access_token"].as_str().unwrap();
    let ini_path = stack.root.join("m1_config.ini");
    let mut ini = ini::Ini::load_from_file(&ini_path).unwrap();
    drop(stack.metadata.take());
    std::fs::rename(stack.root.join("m1.env"), stack.root.join("metadata.env")).unwrap();
    ini.with_section(Some("server"))
        .set("env_file", "metadata.env");
    ini.with_section(Some("set_limits"))
        .set("max_file_size", "262144");
    ini.write_to_file(&ini_path).unwrap();
    stack.start_metadata().await;
    let bytes = vec![37; 262145];
    for unknown in [false, true] {
        let path = format!("rejected-{unknown}");
        let body = if unknown {
            streamed(&bytes)
        } else {
            bytes.clone().into()
        };
        let response = client
            .post(format!("{base}/upload/one-shot/{path}"))
            .bearer_auth(token)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 413);
        assert_eq!(
            client
                .get(format!("{base}/objects/get/{path}"))
                .bearer_auth(token)
                .send()
                .await
                .unwrap()
                .status(),
            404
        );
    }
    for durable in [false, true] {
        let route = if durable {
            "multipart/durable"
        } else {
            "multipart"
        };
        let response = client
            .post(format!("{base}/upload/{route}/init/rejected-multipart"))
            .bearer_auth(token)
            .json(&json!({"total_size_bytes":262145,"part_size_bytes":65536,"expected_parts":5}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 413);
    }
    let form = reqwest::multipart::Form::new()
        .part(
            "object",
            reqwest::multipart::Part::bytes(bytes).file_name("rejected-batch"),
        )
        .part(
            "object",
            reqwest::multipart::Part::bytes(vec![19; 262144]).file_name("cap-boundary"),
        );
    let batch = json_response(
        client
            .post(format!("{base}/upload/batch"))
            .bearer_auth(token)
            .multipart(form)
            .send()
            .await
            .unwrap(),
        207,
    )
    .await;
    assert_eq!(batch["uploaded"], 1);
    assert_eq!(batch["failed"], 1);
    assert!(
        batch["results"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["object_key"] == "rejected-batch" && r["status"] == 413)
    );
    let read = client
        .get(format!("{base}/objects/get/cap-boundary"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(read.status(), 200);
    assert_eq!(read.bytes().await.unwrap().as_ref(), vec![19; 262144]);
    // Unset and explicit unlimited are both exercised through real restarts.
    for explicit in [false, true] {
        drop(stack.metadata.take());
        ini.with_section(Some("set_limits"))
            .delete(&"max_file_size");
        if explicit {
            ini.with_section(Some("set_limits"))
                .set("max_file_size", "unlimited");
        }
        if explicit {
            ini.with_section(Some("server")).delete(&"env_file");
        }
        ini.write_to_file(&ini_path).unwrap();
        if explicit {
            let credentials: Value = serde_json::from_slice(
                &std::fs::read(stack.object_config.with_extension("credentials.json")).unwrap(),
            )
            .unwrap();
            stack
                .start_metadata_with_env(&[
                    ("JWT_SECRET", "test-only-exported-metadata-jwt"),
                    ("AUTH_KEY", credentials["secret"].as_str().unwrap()),
                    ("MASTER_KEY", "test-only-master"),
                    ("ADMIN_KEY", "test-only-admin"),
                ])
                .await;
        } else {
            stack.start_metadata().await;
        }
        // Renew after selecting different JWT credentials for the service.
        let logged = json_response(
            client
                .post(format!("{base}/auth/fragment"))
                .json(&json!({
                    "fragment": created["fragment_hash"],
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
        let token = logged["access_token"].as_str().unwrap();
        transfers(&client, &base, token, &format!("unlimited-{explicit}")).await;
        large_sessions(&client, &base, token, &format!("large-{explicit}")).await;
        assert!(!stack.root.join("m1.env").exists());
    }
    std::fs::write(
        stack.root.join("SIZE-POLICY-PASSED.json"),
        serde_json::to_vec_pretty(&json!({
            "passed": true,
            "finite_cap_bytes": 262144,
            "real_payload_bytes": 2097159,
            "stream_modes": ["content-length","chunked"],
            "large_admission_bytes": [15_u64<<30,1_u64<<40],
            "unlimited_modes": ["omitted","explicit"],
            "large_payloads_transferred": false,
            "part_size_and_count_limits_enforced": true
        }))
        .unwrap(),
    )
    .unwrap();
}

#[tokio::test]
#[ignore = "explicit opt-in against an existing local test deployment"]
async fn running_deployment_unlimited_upload_smoke() {
    let access: Value = serde_json::from_slice(
        &std::fs::read(
            std::env::var_os("N1_SIZE_TEST_ACCESS_FILE").expect("private access file required"),
        )
        .unwrap(),
    )
    .unwrap();
    let ca = reqwest::Certificate::from_pem(
        &std::fs::read(std::env::var_os("N1_SIZE_TEST_CA").expect("test CA required")).unwrap(),
    )
    .unwrap();
    let client = Client::builder()
        .timeout(Duration::from_secs(180))
        .add_root_certificate(ca)
        .build()
        .unwrap();
    let base = format!(
        "{}/noa/v1",
        access["metadata_url"]
            .as_str()
            .unwrap()
            .trim_end_matches('/')
    );
    let logged = json_response(
        client
            .post(format!("{base}/auth/fragment"))
            .json(&json!({
                "fragment": access["fragment"],
                "secret": access["fragment_key"],
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
    let token = logged["access_token"].as_str().unwrap();
    let prefix = format!("__n1-size-test/{}", ulid::Ulid::new());
    transfers(&client, &base, token, &prefix).await;
    large_sessions(&client, &base, token, &prefix).await;
    if let Some(report) = std::env::var_os("N1_SIZE_TEST_REPORT") {
        std::fs::write(
            report,
            serde_json::to_vec_pretty(&json!({
                "passed": true,
                "payload_bytes": 2097159,
                "byte_verified_transfers": 2,
                "large_admission_bytes": [15_u64<<30,1_u64<<40],
                "large_session_modes": ["standard","durable"],
                "resume_and_cancel": true,
                "large_payloads_transferred": false,
                "part_size_and_count_limits_enforced": true
            }))
            .unwrap(),
        )
        .unwrap();
    }
}

#[tokio::test]
#[ignore = "explicit large-payload round trip against the local test deployment"]
async fn running_deployment_large_multipart_round_trip() {
    use futures_util::StreamExt;
    let access: Value = serde_json::from_slice(
        &std::fs::read(
            std::env::var_os("N1_SIZE_TEST_ACCESS_FILE").expect("private access file required"),
        )
        .unwrap(),
    )
    .unwrap();
    let ca = reqwest::Certificate::from_pem(
        &std::fs::read(std::env::var_os("N1_SIZE_TEST_CA").expect("test CA required")).unwrap(),
    )
    .unwrap();
    let total: u64 = std::env::var("N1_SIZE_TEST_BYTES")
        .expect("explicit large payload size required")
        .parse()
        .unwrap();
    assert!(total > 5_u64 << 30 && total <= 1_u64 << 40);
    let part = 32_u64 << 20;
    let client = Client::builder()
        .timeout(Duration::from_secs(1800))
        .add_root_certificate(ca)
        .build()
        .unwrap();
    let base = format!(
        "{}/noa/v1",
        access["metadata_url"]
            .as_str()
            .unwrap()
            .trim_end_matches('/')
    );
    let logged = json_response(
        client
            .post(format!("{base}/auth/fragment"))
            .json(&json!({
                "fragment": access["fragment"],
                "secret": access["fragment_key"],
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
    let mut token = logged["access_token"].as_str().unwrap().to_owned();
    let path = format!("__n1-size-test/{}/large-round-trip.bin", ulid::Ulid::new());
    let shape = json!({
        "content_type": "application/octet-stream",
        "total_size_bytes": total,
        "part_size_bytes": part,
        "expected_parts": total.div_ceil(part)
    });
    let upload = json_response(
        client
            .post(format!("{base}/upload/multipart/durable/init/{path}"))
            .bearer_auth(&token)
            .json(&shape)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let version = upload["version_id"].as_str().unwrap();
    let session = upload["session_id"].as_str().unwrap();
    let pattern = (0..65536).map(|n| (n % 251) as u8).collect::<Vec<_>>();
    let mut payload = pattern.repeat(part as usize / pattern.len());
    let started = std::time::Instant::now();
    let mut auth_at = std::time::Instant::now();
    for index in 0..total.div_ceil(part) {
        if auth_at.elapsed() > Duration::from_secs(300) {
            let logged = json_response(
                client
                    .post(format!("{base}/auth/fragment"))
                    .json(&json!({
                        "fragment": access["fragment"],
                        "secret": access["fragment_key"],
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
            token = logged["access_token"].as_str().unwrap().to_owned();
            auth_at = std::time::Instant::now();
        }
        payload[..8].copy_from_slice(&index.to_be_bytes());
        let length = (total - index * part).min(part) as usize;
        json_response(
            client
                .put(format!("{base}/upload/multipart/durable/{version}/{index}"))
                .bearer_auth(&token)
                .body(bytes::Bytes::copy_from_slice(&payload[..length]))
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        if (index + 1).is_multiple_of(32) {
            eprintln!(
                "large upload: {} bytes acknowledged",
                ((index + 1) * part).min(total)
            );
        }
    }
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
    let upload_seconds = started.elapsed().as_secs_f64();
    let started = std::time::Instant::now();
    let response = client
        .get(format!("{base}/objects/get/{path}"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.content_length(), Some(total));
    let mut stream = response.bytes_stream();
    let mut offset = 0_u64;
    let mut last_report = 0;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        let mut checked = 0;
        while checked < chunk.len() {
            let index = offset / part;
            payload[..8].copy_from_slice(&index.to_be_bytes());
            let within = (offset % part) as usize;
            let count = (chunk.len() - checked).min(payload.len() - within);
            assert_eq!(
                &chunk[checked..checked + count],
                &payload[within..within + count],
                "payload mismatch at byte {offset}"
            );
            checked += count;
            offset += count as u64;
            assert!(offset <= total);
        }
        if offset / (1_u64 << 30) > last_report {
            last_report = offset / (1_u64 << 30);
            eprintln!("large download: {offset} bytes verified");
        }
    }
    assert_eq!(offset, total);
    let download_seconds = started.elapsed().as_secs_f64();
    let report = json!({
        "passed": true,
        "bytes": total,
        "part_bytes": part,
        "parts": total.div_ceil(part),
        "durability": "strict",
        "every_downloaded_byte_compared": true,
        "repeated_finalize_identical": true,
        "upload_seconds": upload_seconds,
        "download_seconds": download_seconds,
        "upload_mib_per_second": total as f64 / 1048576.0 / upload_seconds,
        "download_mib_per_second": total as f64 / 1048576.0 / download_seconds,
        "retained_named_fixture": path
    });
    if let Some(path) = std::env::var_os("N1_SIZE_TEST_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    }
    eprintln!("{report}");
}
