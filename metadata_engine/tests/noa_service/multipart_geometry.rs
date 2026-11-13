//! Negotiation changes geometry before allocating LINK, never during resume.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated real N1/M1 processes and storage roots"]
async fn negotiated_init_streams_without_staging_and_reuses_identity_after_restart() {
    let mut stack = Stack::with_block_bytes(262144).await;
    let client = Client::new();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token) = reliability::fragment(&client, &base, "negotiated geometry").await;
    let payload: Vec<_> = (0..700003).map(|i| (i * 31 % 251) as u8).collect();
    let route = format!("{base}/upload/multipart/durable");
    let hint = json!({"total_size_bytes":payload.len(),"expected_parts":3,
        "part_size_bytes":300001,"negotiate_part_size":true});
    let init_url = format!("{route}/init/negotiated.bin?durability=strict");
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
    assert_eq!(upload["part_size_bytes"], 262144);
    assert_eq!(upload["expected_parts"], 3);
    let version = upload["version_id"].as_str().unwrap();
    resume_progress::put(
        &client,
        &route,
        &token,
        version,
        0,
        payload[..262144].to_vec(),
    )
    .await;
    drop(stack.metadata.take());
    drop(stack.object.take());
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
    let state = resume_progress::status(&client, &route, &token, version).await;
    assert_eq!(state["part_size_bytes"], 262144);
    assert_eq!(state["uploaded_parts"], json!([0]));
    for i in [2, 1] {
        resume_progress::put(
            &client,
            &route,
            &token,
            version,
            i as u64,
            payload[i * 262144..payload.len().min((i + 1) * 262144)].to_vec(),
        )
        .await;
    }
    let settings = m1::server::ServerConfig::load(&stack.root.join("m1_config.ini")).unwrap();
    // Aligned parts flow directly to N1. No payload-sized M1 spool is created.
    fn files(path: &Path) -> usize {
        std::fs::read_dir(path)
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                if path.is_dir() { files(&path) } else { 1 }
            })
            .sum()
    }
    assert_eq!(files(&settings.staging_dir), 0);
    let finish = format!(
        "{route}/{version}/{}/finalize",
        upload["session_id"].as_str().unwrap()
    );
    for _ in 0..2 {
        let done = json_response(
            client
                .post(&finish)
                .bearer_auth(&token)
                .json(&json!({"total_size_bytes":payload.len(),"expected_parts":3}))
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        assert_eq!(done["version_id"], version);
    }
    let get = client
        .get(format!("{base}/objects/get/negotiated.bin"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 200);
    assert_eq!(get.bytes().await.unwrap().as_ref(), payload);

    // Explicit callers retain arbitrary geometry. Native clients can opt in
    // through the same NOA operation without a separate discovery request.
    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", stack.native))
        .await
        .unwrap();
    assert!(
        native(&mut socket, "auth.bearer", json!({"token":token}), None)
            .await
            .0
            .ok
    );
    let mut original = None;
    for negotiate in [false, true, true] {
        let (response, body) = native(
            &mut socket,
            "multipart.init",
            json!({
                "object_key":"native-explicit.bin","total_size_bytes":700003,
                "expected_parts":3,"part_size_bytes":300001,"negotiate_part_size":negotiate
            }),
            None,
        )
        .await;
        assert!(response.ok, "{:?}", response.error);
        let handle: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(handle["part_size_bytes"], 300001);
        if let Some(original) = &original {
            assert_eq!(&handle, original);
        } else {
            original = Some(handle);
        }
    }
    let bad = client
        .post(format!("{route}/init/bad-shape"))
        .bearer_auth(&token)
        .json(&json!({"total_size_bytes":700003,
            "expected_parts":2,"part_size_bytes":300001,"negotiate_part_size":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);
}
