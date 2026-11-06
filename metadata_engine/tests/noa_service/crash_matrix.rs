//! Kill owned real processes while an HTTP multipart body is deliberately
//! incomplete. Every earlier ACK remains an external, immutable readback oracle.
use super::reliability::{fragment, record};
use super::resume_progress::{put, status};
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "private two-HDD fault campaign: six in-flight metadata/object/both SIGKILL cases"]
async fn inflight_process_crash_matrix_preserves_all_prior_acks_and_only_resumes_missing_parts() {
    let mut stack = Stack::new().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token) = fragment(&client, &base, "Inflight process crashes").await;
    let route = format!("{base}/upload/multipart/durable");
    let shape = json!({"total_size_bytes":262161,"part_size_bytes":131072,"expected_parts":3});
    let mut acknowledged = Vec::new();
    let mut cases = Vec::new();
    for (case, (target, prefix)) in [
        ("metadata", 4096),
        ("metadata", 65536),
        ("object", 4096),
        ("object", 65536),
        ("both", 4096),
        ("both", 65536),
    ]
    .into_iter()
    .enumerate()
    {
        let path = format!("crashes/{case}.bin");
        let upload = json_response(
            client
                .post(format!("{route}/init/{path}?durability=strict"))
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
        let first = vec![51 + case as u8; 131072];
        let middle = vec![71 + case as u8; 131072];
        let tail = vec![91 + case as u8; 17];
        put(&client, &route, &token, version, 0, first.clone()).await;
        assert_eq!(
            status(&client, &route, &token, version).await["uploaded_parts"],
            json!([0])
        );
        record(
            &stack.root.join(format!("before-crash-{case}.json")),
            &json!({
                "target": target,
                "session": session,
                "version": version,
                "acknowledged_parts": [0],
                "incomplete_part": 1,
                "body_prefix_bytes": prefix,
                "body_expected_bytes": middle.len(),
                "prior_complete_objects": acknowledged.len()
            }),
        );
        let mut socket = tokio::net::TcpStream::connect(stack.url.trim_start_matches("http://"))
            .await
            .unwrap();
        socket.write_all(format!(
            "PUT /noa/v1/upload/multipart/durable/{version}/1 HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\nContent-Length: 131072\r\nConnection: close\r\n\r\n"
        ).as_bytes()).await.unwrap();
        socket.write_all(&middle[..prefix]).await.unwrap();
        // The source remains open and incomplete throughout the injected kill.
        tokio::time::sleep(Duration::from_millis(150)).await;
        if target == "metadata" || target == "both" {
            drop(stack.metadata.take());
        }
        if target == "object" || target == "both" {
            drop(stack.object.take());
        }
        drop(socket);
        if stack.object.is_none() {
            stack.start_object().await;
        }
        if stack.metadata.is_none() {
            stack.start_metadata().await;
        }
        let recovered = status(&client, &route, &token, version).await;
        assert_eq!(
            recovered["uploaded_parts"],
            json!([0]),
            "incomplete request was acknowledged: {target}/{prefix}"
        );
        let reused = json_response(
            client
                .post(format!("{route}/init/{path}?durability=strict"))
                .bearer_auth(&token)
                .json(&shape)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        assert_eq!(reused, upload, "process crash changed upload identity");
        let rejected = client
            .put(format!("{route}/{version}/0"))
            .bearer_auth(&token)
            .body(vec![211; first.len()])
            .send()
            .await
            .unwrap();
        assert_eq!(
            rejected.status(),
            409,
            "conflicting retry overwrote an acknowledged part"
        );
        put(&client, &route, &token, version, 1, middle.clone()).await;
        put(&client, &route, &token, version, 2, tail.clone()).await;
        let mut final_ack = None;
        for _ in 0..2 {
            let result = json_response(
                client
                    .post(format!("{route}/{version}/{session}/finalize"))
                    .bearer_auth(&token)
                    .json(&shape)
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await;
            if let Some(original) = &final_ack {
                assert_eq!(*original, result);
            }
            final_ack = Some(result);
        }
        let expected = [first, middle, tail].concat();
        acknowledged.push((path, version.to_owned(), expected));
        // Each new fault must preserve every earlier completed object too.
        for (path, version, bytes) in &acknowledged {
            let response = client
                .get(format!("{base}/objects/get/{path}"))
                .bearer_auth(&token)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            assert_eq!(
                response.headers()["etag"]
                    .to_str()
                    .unwrap()
                    .trim_matches('"'),
                version
            );
            assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
        }
        cases.push(json!({
            "target": target,
            "partial_body_bytes": prefix,
            "prior_and_current_objects_verified": acknowledged.len(),
            "passed": true
        }));
        eprintln!(
            "In-flight crash passed: {target}, {prefix} source bytes, {} exact objects",
            acknowledged.len()
        );
    }
    drop(stack.metadata.take());
    drop(stack.object.take());
    stack.start_object().await;
    stack.start_metadata().await;
    for (path, version, bytes) in &acknowledged {
        let response = client
            .get(format!("{base}/objects/get/{path}"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.headers()["etag"]
                .to_str()
                .unwrap()
                .trim_matches('"'),
            version
        );
        assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
    }
    record(
        &stack.root.join("INFLIGHT-CRASH-PASSED.json"),
        &json!({
            "passed": true,
            "cases": cases,
            "completed_objects": acknowledged.len(),
            "incomplete_request_acknowledged": false,
            "acknowledged_parts_lost": false,
            "conflicting_retry_overwrote_data": false,
            "final_cold_restart_verified": true
        }),
    );
}
