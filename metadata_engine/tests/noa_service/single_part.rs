//! A single external part needs no payload conversion or M1 spool.
use super::*;

fn payload_files(path: &Path) -> usize {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            if path.is_dir() {
                payload_files(&path)
            } else {
                1
            }
        })
        .sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real single-external-part boundary above the aligned native maximum, plus empty upload"]
async fn single_external_part_retains_bridge_when_it_exceeds_native_part_capacity() {
    let mut stack = Stack::with_block_bytes(262144).await;
    drop(stack.metadata.take());
    let config = stack.root.join("m1_config.ini");
    let mut ini = ini::Ini::load_from_file(&config).unwrap();
    ini.with_section(Some("upload_config"))
        .set("max_part_size", "300001");
    ini.write_to_file(&config).unwrap();
    let staging = m1::server::ServerConfig::load(&config).unwrap().staging_dir;
    stack.start_metadata().await;
    let client = Client::new();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token) = reliability::fragment(&client, &base, "Native capacity boundary").await;
    let route = format!("{base}/upload/multipart/durable");
    for (name, size) in [("empty.bin", 0usize), ("beyond-native-part.bin", 300001)] {
        let bytes: Vec<_> = (0..size).map(|i| (i * 31 % 251) as u8).collect();
        let init_url = format!("{route}/init/{name}");
        let hint = json!({"total_size_bytes":size,"part_size_bytes":size,"expected_parts":1});
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
        assert_eq!(upload["part_size_bytes"], size);
        assert_eq!(upload["expected_parts"], 1);
        let version = upload["version_id"].as_str().unwrap();
        resume_progress::put(&client, &route, &token, version, 0, bytes.clone()).await;
        assert_eq!(payload_files(&staging), 1);
        drop(stack.metadata.take());
        drop(stack.object.take());
        stack.start_object().await;
        stack.start_metadata().await;
        let saved = json_response(
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
        assert_eq!(saved, upload);
        assert_eq!(
            resume_progress::status(&client, &route, &token, version).await["uploaded_parts"],
            json!([0])
        );
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
        let read = client
            .get(format!("{base}/objects/get/{name}"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(read.status(), StatusCode::OK);
        assert_eq!(read.bytes().await.unwrap().as_ref(), bytes);
        assert_eq!(payload_files(&staging), 0);
    }
    reliability::record(
        &stack.root.join("SINGLE-PART-BOUNDARY-PASSED.json"),
        &json!({"passed":true,"external_part_maximum":300001,
        "native_part_maximum":262144,"one_external_part_to_two_native_parts":true,"empty":true}),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real N1/M1, single-part direct streaming, retries, process restart and HTTP/native contracts"]
async fn unaligned_single_parts_stream_without_m1_staging_and_preserve_client_geometry() {
    let mut stack = Stack::with_block_bytes(262144).await;
    let client = Client::new();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token) = reliability::fragment(&client, &base, "Single-part geometry").await;
    let staging = m1::server::ServerConfig::load(&stack.root.join("m1_config.ini"))
        .unwrap()
        .staging_dir;
    for (size, durable) in [(4097usize, true), (700003, true), (11003, false)] {
        let bytes: Vec<_> = (0..size).map(|i| (i * 31 % 251) as u8).collect();
        let path = format!("single-{size}.bin");
        let route = format!(
            "{base}/upload/multipart{}",
            if durable { "/durable" } else { "" }
        );
        let policy = if durable { "strict" } else { "batched" };
        let hint = json!({"total_size_bytes":size,"part_size_bytes":size,"expected_parts":1});
        let init = format!("{route}/init/{path}?durability={policy}");
        let upload = json_response(
            client
                .post(&init)
                .bearer_auth(&token)
                .json(&hint)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        assert_eq!(upload["part_size_bytes"], size);
        assert_eq!(upload["expected_parts"], 1);
        let version = upload["version_id"].as_str().unwrap();
        let part = format!("{route}/{version}/0?durability={policy}");
        json_response(
            client
                .put(&part)
                .bearer_auth(&token)
                .body(bytes.clone())
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        assert_eq!(
            payload_files(&staging),
            0,
            "single-part upload created an M1 spool"
        );
        json_response(
            client
                .put(&part)
                .bearer_auth(&token)
                .body(bytes.clone())
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        let mut wrong = bytes.clone();
        wrong[17] ^= 1;
        assert_eq!(
            client
                .put(&part)
                .bearer_auth(&token)
                .body(wrong)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );
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
        if durable {
            drop(stack.metadata.take());
            drop(stack.object.take());
            stack.start_object().await;
            stack.start_metadata().await;
            let saved = json_response(
                client
                    .post(&init)
                    .bearer_auth(&token)
                    .json(&hint)
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await;
            assert_eq!(saved, upload);
        }
        let state = resume_progress::status(&client, &route, &token, version).await;
        assert_eq!(state["part_size_bytes"], size);
        assert_eq!(state["uploaded_parts"], json!([0]));
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
        drop(stack.metadata.take());
        drop(stack.object.take());
        stack.start_object().await;
        stack.start_metadata().await;
        let read = client
            .get(format!("{base}/objects/get/{path}"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(read.status(), StatusCode::OK);
        assert_eq!(read.bytes().await.unwrap().as_ref(), bytes);
        assert_eq!(payload_files(&staging), 0);
    }
    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", stack.native))
        .await
        .unwrap();
    assert!(
        native(&mut socket, "auth.bearer", json!({"token":token}), None)
            .await
            .0
            .ok
    );
    let (init, body) = native(
        &mut socket,
        "multipart.init",
        json!({
            "object_key":"native-single.bin","durable":true,
            "total_size_bytes":4097,"part_size_bytes":4097,"expected_parts":1
        }),
        None,
    )
    .await;
    assert!(init.ok, "{:?}", init.error);
    let upload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(upload["part_size_bytes"], 4097);
    let bytes = vec![39; 4097];
    let (part, _) = native(
        &mut socket,
        "multipart.put_part",
        json!({
            "version_id":upload["version_id"],"part_index":0,"durable":true
        }),
        Some(&bytes),
    )
    .await;
    assert!(part.ok, "{:?}", part.error);
    assert_eq!(payload_files(&staging), 0);
    let (done, _) = native(
        &mut socket,
        "multipart.finalize",
        json!({
            "version_id":upload["version_id"],"session_id":upload["session_id"],"durable":true
        }),
        None,
    )
    .await;
    assert!(done.ok, "{:?}", done.error);
    let (read, body) = native(
        &mut socket,
        "object.get",
        json!({"object_key":"native-single.bin"}),
        None,
    )
    .await;
    assert!(read.ok, "{:?}", read.error);
    assert_eq!(body, bytes);
    reliability::record(
        &stack.root.join("SINGLE-PART-PASSED.json"),
        &json!({"passed":true,"http_durable_restarts":2,"http_batched":true,
        "native_multipart":true,"source_geometry_preserved":true,"m1_payload_files":0}),
    );
}
