//! Operator quotas must be shared across HTTP/native and token rotations.
use super::*;

fn policy(stack: &Stack, enabled: bool, auth: u32) {
    let path = stack.root.join("m1_config.ini");
    let mut ini = ini::Ini::load_from_file(&path).unwrap();
    ini.with_section(Some("rate_limiter_config"))
        .set("enable_rate_limiting", enabled.to_string())
        .set("global_limit_per_second", "10000")
        .set("global_limit_burst_size", "10000")
        .set("auth_limit_per_second", "1")
        .set("auth_limit_burst_size", auth.to_string())
        .set("view_limit_per_second", "1")
        .set("view_limit_burst_size", "2")
        .set("upload_limit_per_second", "1")
        .set("upload_limit_burst_size", "2");
    ini.write_to_file(path).unwrap();
}

#[tokio::test]
#[ignore = "requires an isolated real N1 binary and explicit storage fixture roots"]
async fn named_request_quotas_apply_to_http_native_tokens_and_peer_spoofing() {
    let mut stack = Stack::new().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_alias, token, other_token) = maintenance::auth(&client, &base, "quotas").await;
    drop(stack.metadata.take());
    policy(&stack, true, 100);
    stack.start_metadata().await;
    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", stack.native))
        .await
        .unwrap();
    assert!(
        native(&mut socket, "auth.bearer", json!({"token":token}), None)
            .await
            .0
            .ok
    );

    assert_eq!(
        client
            .get(format!("{base}/fragments"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert!(
        native(&mut socket, "fragment.list", json!({}), None)
            .await
            .0
            .ok
    );
    let rejected = client
        .get(format!("{base}/fragments"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), 429);
    assert!(rejected.headers().contains_key("retry-after"));
    assert_eq!(
        native(&mut socket, "fragment.list", json!({}), None)
            .await
            .0
            .error
            .as_ref()
            .map(|error| error.code.as_str()),
        Some("429")
    );
    assert_eq!(
        client
            .get(format!("{base}/fragments"))
            .bearer_auth(&other_token)
            .send()
            .await
            .unwrap()
            .status(),
        429
    );

    // Uploads have a separate quota. Rejecting invalid requests does not allocate
    // shards, and the third attempt must fail at admission rather than parsing.
    for expected in [422, 422, 429] {
        let response = client
            .post(format!("{base}/upload/multipart/init/file"))
            .bearer_auth(&token)
            .json(&json!({}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
    }
    let rejected = native(
        &mut socket,
        "upload.one_shot",
        json!({"object_key":"must-not-exist.bin"}),
        Some(b"rejected payload"),
    )
    .await
    .0;
    assert_eq!(
        rejected.error.as_ref().map(|e| e.code.as_str()),
        Some("429")
    );
    drop(socket);
    drop(stack.metadata.take());
    policy(&stack, true, 1);
    stack.start_metadata().await;
    let bad = client
        .post(format!("{base}/auth/fragment"))
        .json(&json!({"fragment":"unknown-fragment","secret":"incorrect"}))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 401);
    let spoof = client
        .post(format!("{base}/auth/fragment"))
        .header("x-forwarded-for", "198.51.100.11")
        .header("x-real-ip", "198.51.100.12")
        .json(&json!({"fragment":"unknown-fragment","secret":"incorrect"}))
        .send()
        .await
        .unwrap();
    assert_eq!(spoof.status(), 429);
    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", stack.native))
        .await
        .unwrap();
    let rejected = native(
        &mut socket,
        "auth.fragment",
        json!({"fragment":"unknown-fragment","secret":"incorrect"}),
        None,
    )
    .await
    .0;
    assert_eq!(
        rejected.error.as_ref().map(|e| e.code.as_str()),
        Some("429")
    );
    drop(socket);
    drop(stack.metadata.take());
    policy(&stack, false, 1);
    stack.start_metadata().await;
    assert_eq!(
        client
            .get(format!("{base}/objects/get/must-not-exist.bin"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    for _ in 0..12 {
        assert_eq!(
            client
                .get(format!("{base}/fragments"))
                .bearer_auth(&token)
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
    }
}
