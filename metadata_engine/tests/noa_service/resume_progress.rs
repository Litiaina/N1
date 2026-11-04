//! Named resume is a durable control-plane query, not a scan of payload bytes.
use super::*;

pub(super) async fn status(client: &Client, route: &str, token: &str, version: &str) -> Value {
    json_response(
        client
            .get(format!("{route}/{version}"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await
}
pub(super) async fn health_submitted(client: &Client, base: &str, token: &str) -> u64 {
    json_response(
        client
            .get(format!("{base}/maintenance/health?live_counters=true"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await["io"]["backend"]["submitted"]
        .as_u64()
        .unwrap()
}
pub(super) async fn put(
    client: &Client,
    route: &str,
    token: &str,
    version: &str,
    index: u64,
    bytes: Vec<u8>,
) {
    json_response(
        client
            .put(format!("{route}/{version}/{index}"))
            .bearer_auth(token)
            .body(bytes)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit object binary and isolated storage roots"]
async fn durable_part_acknowledgements_resume_without_object_reads_after_both_processes_are_killed()
{
    let mut stack = Stack::new().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let created=json_response(client.post(format!("{base}/fragments")).json(&json!({"name":"ACK resume","secret":"test-only-master","key":"test-only-fragment"})).send().await.unwrap(),201).await;
    let logged=json_response(client.post(format!("{base}/auth/fragment")).json(&json!({"fragment":created["fragment_hash"],"secret":"test-only-fragment","read":true,"write":true,"delete":true})).send().await.unwrap(),200).await;
    let token = logged["access_token"].as_str().unwrap();
    for durable in [false, true] {
        let route = format!(
            "{base}/upload/multipart{}",
            if durable { "/durable" } else { "" }
        );
        let shape =
            json!({"total_size_bytes":513*8192+17,"part_size_bytes":8192,"expected_parts":514});
        let upload = json_response(
            client
                .post(format!("{route}/init/ack-{durable}?durability=strict"))
                .bearer_auth(token)
                .json(&shape)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        let version = upload["version_id"].as_str().unwrap();
        let session = upload["session_id"].as_str().unwrap();
        let initial = [0, 255, 256, 512, 513];
        for index in initial {
            put(
                &client,
                &route,
                token,
                version,
                index,
                vec![(index % 251) as u8; if index == 513 { 17 } else { 8192 }],
            )
            .await;
        }
        // A conflicting retry cannot replace the recorded immutable bytes.
        let conflict = client
            .put(format!("{route}/{version}/0"))
            .bearer_auth(token)
            .body(vec![99; 8192])
            .send()
            .await
            .unwrap();
        assert_eq!(conflict.status(), 409);
        // A truncated body may leave valid early frames, but receives no ACK.
        let mut socket = tokio::net::TcpStream::connect(stack.url.trim_start_matches("http://"))
            .await
            .unwrap();
        socket.write_all(format!("PUT /noa/v1/upload/multipart{}/{version}/1 HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\nContent-Length: 8192\r\nConnection: close\r\n\r\n",if durable{"/durable"}else{""}).as_bytes()).await.unwrap();
        socket.write_all(&vec![1; 4096]).await.unwrap();
        socket.shutdown().await.unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), socket.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert!(!response.starts_with(b"HTTP/1.1 200"));
        let before = health_submitted(&client, &base, token).await;
        let first = status(&client, &route, token, version).await;
        assert_eq!(first["uploaded_parts"], json!(initial));
        assert_eq!(
            health_submitted(&client, &base, token).await,
            before,
            "resume submitted object I/O"
        );
        // Kill both actual engine processes; metadata's acknowledged bits must
        // survive without an in-memory object progress cache or upload scan.
        drop(stack.metadata.take());
        drop(stack.object.take());
        stack.start_object().await;
        stack.start_metadata().await;
        let before = health_submitted(&client, &base, token).await;
        assert_eq!(
            status(&client, &route, token, version).await["uploaded_parts"],
            json!(initial)
        );
        assert_eq!(
            health_submitted(&client, &base, token).await,
            before,
            "cold resume scanned object frames"
        );
        let reused = json_response(
            client
                .post(format!("{route}/init/ack-{durable}?durability=strict"))
                .bearer_auth(token)
                .json(&shape)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        assert_eq!(upload, reused);
        // Send only missing parts, then verify the complete reconstructed data.
        for index in 0..513 {
            if !initial.contains(&index) {
                put(
                    &client,
                    &route,
                    token,
                    version,
                    index,
                    vec![(index % 251) as u8; 8192],
                )
                .await;
            }
        }
        let finalize = format!("{route}/{version}/{session}/finalize");
        for _ in 0..2 {
            json_response(
                client
                    .post(&finalize)
                    .bearer_auth(token)
                    .json(&shape)
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await;
        }
        let read = client
            .get(format!("{base}/objects/get/ack-{durable}"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap();
        assert_eq!(read.status(), 200);
        let mut expected = Vec::new();
        for index in 0..514 {
            expected.extend(vec![
                (index % 251) as u8;
                if index == 513 { 17 } else { 8192 }
            ]);
        }
        assert_eq!(read.bytes().await.unwrap().as_ref(), expected);
        assert_eq!(
            status(&client, &route, token, version).await["committed_parts"],
            514
        );
        // Publication atomically removed every progress page.
        drop(stack.metadata.take());
        let config =
            n1_metadata_engine::server::ServerConfig::load(&stack.root.join("n1_config.ini"))
                .unwrap();
        let db = n1_metadata_engine::MetadataEngine::open(
            &config.data_dir,
            n1_metadata_engine::Options::default(),
        )
        .unwrap();
        let fragment =
            n1_metadata_engine::fragments::resolve(&db, created["fragment_hash"].as_str().unwrap())
                .unwrap();
        assert!(
            db.snapshot()
                .scan(
                    n1_metadata_engine::Table::Reclamation,
                    fragment.id,
                    &format!("upload-progress/{version}/"),
                    None,
                    1000
                )
                .unwrap()
                .is_empty()
        );
        drop(db);
        stack.start_metadata().await;
    }
}
