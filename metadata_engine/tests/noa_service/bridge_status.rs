//! Durable bridge status observes receipts while another temporary file grows.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real N1/M1 bridge, controlled incomplete HTTP body and restart verification"]
async fn receipts_remain_queryable_while_another_part_is_still_being_received() {
    let mut stack = Stack::with_block_bytes(262144).await;
    let client = Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token) = reliability::fragment(&client, &base, "Bridge status").await;
    let route = format!("{base}/upload/multipart/durable");
    let part_bytes = 1000001;
    let bytes = vec![41; 2 * part_bytes];
    let upload = json_response(
        client
            .post(format!("{route}/init/bridge.bin"))
            .bearer_auth(&token)
            .json(&json!({"total_size_bytes":bytes.len(),
            "part_size_bytes":part_bytes,"expected_parts":2}))
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
        bytes[..part_bytes].to_vec(),
    )
    .await;
    let pid = stack.metadata.as_ref().unwrap().0.id();
    let before_idle = preparation_bench::resources(pid);
    let idle = stalled_io::measure_status(&client, &route, &token, version).await;
    let after_idle = preparation_bench::resources(pid);
    reliability::record(
        &stack.root.join("BRIDGE-STATUS-IDLE.json"),
        &json!({"latency":idle,"before":before_idle,"after":after_idle}),
    );
    let settings = m1::server::ServerConfig::load(&stack.root.join("m1_config.ini")).unwrap();
    let url = reqwest::Url::parse(&stack.url).unwrap();
    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", url.port().unwrap()))
        .await
        .unwrap();
    let header = format!(
        "PUT /noa/v1/upload/multipart/durable/{version}/1 HTTP/1.1\r\n\
        Host: localhost\r\nAuthorization: Bearer {token}\r\n\
        Content-Length: {part_bytes}\r\nConnection: close\r\n\r\n"
    );
    socket.write_all(header.as_bytes()).await.unwrap();
    socket.write_all(&bytes[..4096]).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while multipart_bridge::temporary_parts(&settings.staging_dir, version).len() != 1 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "bridge body did not enter staging"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let before_active = preparation_bench::resources(pid);
    let active = stalled_io::measure_status(&client, &route, &token, version).await;
    let after_active = preparation_bench::resources(pid);
    reliability::record(
        &stack.root.join("BRIDGE-STATUS-ACTIVE.json"),
        &json!({"latency":active,"before":before_active,"after":after_active}),
    );
    socket.write_all(&bytes[4096..part_bytes]).await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), socket.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(
        response.starts_with(b"HTTP/1.1 200 "),
        "bridge part did not ACK"
    );
    let finish = format!(
        "{route}/{version}/{}/finalize",
        upload["session_id"].as_str().unwrap()
    );
    for _ in 0..2 {
        json_response(
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
    }
    drop(stack.metadata.take());
    drop(stack.object.take());
    stack.start_object().await;
    stack.start_metadata().await;
    let read = client
        .get(format!("{base}/objects/get/bridge.bin"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(read.status(), StatusCode::OK);
    assert_eq!(
        read.headers()["etag"].to_str().unwrap().trim_matches('"'),
        version
    );
    assert_eq!(read.bytes().await.unwrap().as_ref(), bytes);
    reliability::record(
        &stack.root.join("BRIDGE-STATUS-PASSED.json"),
        &json!({"passed":true,"idle_samples":100,"active_samples":100,
        "unacknowledged_temporary_file_excluded":true,"restart_full_payload_and_version":true}),
    );
}
