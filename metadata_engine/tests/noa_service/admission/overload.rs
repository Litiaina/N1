//! Sustained mixed-transport admission uses one canonical fragment budget.
//! Verify every payload acknowledged under pressure after reopening both stores.
use super::*;
use std::sync::Arc;

#[derive(Default)]
struct Counts {
    accepted: u64,
    rejected: u64,
    uploaded: Vec<(String, String)>,
    milliseconds: Vec<f64>,
}

async fn connection(port: u16, token: &str) -> tokio::net::TcpStream {
    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    assert!(
        native(&mut socket, "auth.bearer", json!({"token":token}), None)
            .await
            .0
            .ok
    );
    socket
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "ten-second mixed HTTP/native overload against private physical N1/M1 fixtures"]
async fn sustained_http_native_overload_preserves_acks_and_shared_quotas() {
    let mut stack = Stack::with_block_bytes(262144).await;
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token, rotated) = maintenance::auth(&client, &base, "sustained quotas").await;
    let payload = bytes::Bytes::from(vec![0x57; 4096]);
    maintenance::put(&client, &base, &token, "seed.bin", &payload).await;
    drop(stack.metadata.take());
    let path = stack.root.join("m1_config.ini");
    let mut ini = ini::Ini::load_from_file(&path).unwrap();
    ini.with_section(Some("rate_limiter_config"))
        .set("enable_rate_limiting", "true")
        .set("global_limit_per_second", "100000")
        .set("global_limit_burst_size", "100000")
        .set("auth_limit_per_second", "10000")
        .set("auth_limit_burst_size", "10000")
        .set("view_limit_per_second", "40")
        .set("view_limit_burst_size", "16")
        .set("upload_limit_per_second", "8")
        .set("upload_limit_burst_size", "4");
    ini.write_to_file(path).unwrap();
    stack.start_metadata().await;
    let barrier = Arc::new(tokio::sync::Barrier::new(17));
    let mut tasks = Vec::new();
    let started = std::time::Instant::now();
    for worker in 0..16 {
        let client = client.clone();
        let base = base.clone();
        let token = if worker % 2 == 0 {
            token.clone()
        } else {
            rotated.clone()
        };
        let barrier = barrier.clone();
        let payload = payload.clone();
        let native_port = stack.native;
        tasks.push(tokio::spawn(async move {
            let is_native = worker % 4 >= 2;
            let is_upload = worker >= 8;
            let mut socket = if is_native {
                Some(connection(native_port, &token).await)
            } else {
                None
            };
            barrier.wait().await;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            let mut counts = Counts::default();
            let mut index = 0;
            while tokio::time::Instant::now() < deadline {
                let name = format!("w{worker:02}-n{index:06}.bin");
                let request_start = std::time::Instant::now();
                let (status, body) = if let Some(socket) = &mut socket {
                    let (response, body) = if is_upload {
                        native(
                            socket,
                            "upload.one_shot",
                            json!({"object_key":name}),
                            Some(&payload),
                        )
                        .await
                    } else {
                        native(socket, "object.get", json!({"object_key":"seed.bin"}), None).await
                    };
                    let status = response
                        .status
                        .unwrap_or_else(|| response.error.as_ref().unwrap().code.parse().unwrap());
                    (status, body)
                } else {
                    let response = if is_upload {
                        client
                            .post(format!("{base}/upload/one-shot/{name}"))
                            .bearer_auth(&token)
                            .body(payload.clone())
                            .send()
                            .await
                            .unwrap()
                    } else {
                        client
                            .get(format!("{base}/objects/get/seed.bin"))
                            .bearer_auth(&token)
                            .send()
                            .await
                            .unwrap()
                    };
                    let status = response.status().as_u16();
                    if status == 429 {
                        assert!(response.headers().contains_key("retry-after"));
                    }
                    (status, response.bytes().await.unwrap().to_vec())
                };
                counts
                    .milliseconds
                    .push(request_start.elapsed().as_secs_f64() * 1000.0);
                if status == 429 {
                    counts.rejected += 1;
                    if is_native && is_upload {
                        // Rejected raw bodies close the exchange so unread
                        // payload cannot become a subsequent control frame.
                        drop(socket.take());
                        socket = Some(connection(native_port, &token).await);
                    }
                } else {
                    assert_eq!(status, if is_upload { 201 } else { 200 });
                    if is_upload {
                        let published: Value = serde_json::from_slice(&body).unwrap();
                        counts
                            .uploaded
                            .push((name, published["version_id"].as_str().unwrap().to_owned()));
                    } else {
                        assert_eq!(body, payload);
                    }
                    counts.accepted += 1;
                }
                index += 1;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            (is_native, is_upload, counts)
        }));
    }
    barrier.wait().await;
    let mut rows = Vec::new();
    let mut uploads = Vec::new();
    let (mut reads, mut writes) = (0, 0);
    for task in tasks {
        let (is_native, is_upload, mut counts) = task.await.unwrap();
        assert!(counts.rejected > 0, "worker did not exercise overload");
        if is_upload {
            writes += counts.accepted;
        } else {
            reads += counts.accepted;
        }
        uploads.append(&mut counts.uploaded);
        counts.milliseconds.sort_by(f64::total_cmp);
        let rank = |percent: usize| {
            counts.milliseconds[(counts.milliseconds.len() * percent).div_ceil(100) - 1]
        };
        rows.push(
            json!({"native":is_native,"upload":is_upload,"accepted":counts.accepted,
            "rejected":counts.rejected,"p50_ms":rank(50),"p95_ms":rank(95),"p99_ms":rank(99)}),
        );
    }
    let elapsed = started.elapsed().as_secs_f64();
    assert!(reads >= 200 && writes >= 40);
    assert!(reads <= 16 + (40.0 * elapsed).ceil() as u64);
    assert!(writes <= 4 + (8.0 * elapsed).ceil() as u64);
    drop(stack.metadata.take());
    drop(stack.object.take());
    policy(&stack, false, 1);
    {
        let db = maintenance::db(&stack);
        let view = db.snapshot();
        let fragments = view
            .scan(
                m1::Table::Reclamation,
                n1_link::PoolId([0; 16]),
                "fragment/",
                None,
                2,
            )
            .unwrap();
        assert_eq!(fragments.len(), 1);
        let catalog: Value = serde_json::from_slice(&fragments[0].1).unwrap();
        let id = serde_json::from_value(catalog["id"].clone()).unwrap();
        assert!(uploads.len() < 200);
        let rows = view.scan(m1::Table::Versions, id, "", None, 1000).unwrap();
        assert!(
            rows.len() < 1000,
            "fixture census must fit one bounded page"
        );
        let stored_versions = rows
            .iter()
            .filter(|(_, bytes)| {
                let row: Value = serde_json::from_slice(bytes).unwrap();
                row["object_key"].is_string()
            })
            .count();
        assert_eq!(
            stored_versions,
            uploads.len() + 1,
            "unacknowledged publication or lost ACK"
        );
        // M1 retains completed handles for idempotent finalize. They are not
        // pending uploads; require exactly the ACKed identities and no others.
        let handles = view.scan(m1::Table::Uploads, id, "", None, 1000).unwrap();
        assert_eq!(handles.len(), uploads.len() + 1);
        for (_, bytes) in handles {
            let handle: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(handle["completed"], true, "unacknowledged staging remained");
        }
        assert!(
            view.scan(m1::Table::Reclamation, id, "pending/", None, 1000)
                .unwrap()
                .is_empty()
        );
    }
    stack.start_object().await;
    stack.start_metadata().await;
    for (name, version) in uploads {
        let response = client
            .get(format!("{base}/objects/get/{name}?version_id={version}"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["etag"], format!("\"{version}\""));
        assert_eq!(response.bytes().await.unwrap(), payload);
    }
    reliability::record(
        &stack.root.join("SUSTAINED-QUOTAS-PASSED.json"),
        &json!({"passed":true,"seconds":elapsed,"read_acks":reads,"write_acks":writes,
            "workers":rows,"post_both_process_kill_all_write_acks_verified":true,
            "namespace_count_exact":true,"shared_view_rate":40,"shared_upload_rate":8}),
    );
}
