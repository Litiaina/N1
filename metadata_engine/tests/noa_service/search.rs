//! Native M1 search against real engines, without an Accel service.
use super::*;
use std::time::Instant;

async fn page(client: &Client, base: &str, token: &str, query: &[(&str, &str)]) -> Value {
    let response = client
        .get(format!("{base}/namespace/search"))
        .bearer_auth(token)
        .query(query)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["x-m1-search"], "native");
    response.json().await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real M1/N1 binaries, private storage, explicit Console build and browser"]
async fn native_search_permissions_pagination_restart_and_console_without_accel() {
    let mut stack = Stack::new().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (fragment, token, _) = maintenance::auth(&client, &base, "Native search").await;
    let (_, other, _) = maintenance::auth(&client, &base, "Search isolation").await;
    let bytes = vec![37; 131_199];
    let version = maintenance::put(&client, &base, &token, "docs/Report.TXT", &bytes).await;
    for path in [
        "docs/sub/report.PDF",
        "docs2/report.txt",
        "media/東京.MP4",
        "hide/trashed.txt",
    ] {
        maintenance::put(&client, &base, &token, path, &bytes[..31]).await;
    }
    maintenance::put(&client, &base, &token, "zero.bin", &[]).await;
    maintenance::put(&client, &base, &other, "docs/SecretReport.txt", &bytes).await;
    json_response(
        client
            .delete(format!("{base}/objects/hide/trashed.txt"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    json_response(
        client
            .post(format!("{base}/upload/multipart/durable/init/pending.bin"))
            .bearer_auth(&token)
            .json(&json!({"total_size_bytes":4096,"part_size_bytes":262144,"expected_parts":1}))
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;

    assert_eq!(
        client
            .get(format!("{base}/namespace/search?q=report"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    let denied = json_response(client.post(format!("{base}/auth/fragment"))
        .json(&json!({"fragment":fragment,"secret":"test-only-fragment","read":false,"write":true,"modify":false,"delete":false}))
        .send().await.unwrap(),200).await;
    assert_eq!(
        client
            .get(format!("{base}/namespace/search?q=report"))
            .bearer_auth(denied["access_token"].as_str().unwrap())
            .send()
            .await
            .unwrap()
            .status(),
        403
    );

    let io_before = resume_progress::health_submitted(&client, &base, &token).await;
    let first = page(
        &client,
        &base,
        &token,
        &[("q", "REPORT"), ("scope", "/docs/"), ("limit", "1")],
    )
    .await;
    assert_eq!(first["results"].as_array().unwrap().len(), 1);
    assert_eq!(first["results"][0]["object_key"], "docs/Report.TXT");
    assert_eq!(first["results"][0]["version_id"], version);
    assert!(first["total_matches"].is_null());
    let cursor = first["continuation_token"].as_str().unwrap().to_owned();
    for (credential, q) in [(&token, "other"), (&other, "REPORT")] {
        assert_eq!(
            client
                .get(format!("{base}/namespace/search"))
                .bearer_auth(credential)
                .query(&[
                    ("q", q),
                    ("scope", "/docs/"),
                    ("limit", "1"),
                    ("continuation_token", cursor.as_str())
                ])
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
    }
    let changed = format!("{cursor}x");
    assert_eq!(
        client
            .get(format!("{base}/namespace/search"))
            .bearer_auth(&token)
            .query(&[
                ("q", "REPORT"),
                ("scope", "/docs/"),
                ("limit", "1"),
                ("continuation_token", changed.as_str())
            ])
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    let second = page(
        &client,
        &base,
        &token,
        &[
            ("q", "REPORT"),
            ("scope", "/docs/"),
            ("limit", "1"),
            ("continuation_token", cursor.as_str()),
        ],
    )
    .await;
    assert_eq!(second["results"][0]["object_key"], "docs/sub/report.PDF");
    if let Some(next) = second["continuation_token"].as_str() {
        let tail = page(
            &client,
            &base,
            &token,
            &[
                ("q", "REPORT"),
                ("scope", "/docs/"),
                ("limit", "1"),
                ("continuation_token", next),
            ],
        )
        .await;
        assert!(tail["results"].as_array().unwrap().is_empty());
        assert!(tail["continuation_token"].is_null());
    }
    let ext = page(
        &client,
        &base,
        &token,
        &[("q", ".MP4"), ("field", "extension"), ("match", "exact")],
    )
    .await;
    assert_eq!(ext["results"][0]["object_key"], "media/東京.MP4");
    let all = page(&client, &base, &token, &[("q", "."), ("field", "filename")]).await;
    let all = all["results"].as_array().unwrap();
    assert_eq!(all.len(), 5);
    assert!(!all.iter().any(|v| v["object_key"] == "pending.bin"
        || v["object_key"] == "hide/trashed.txt"
        || v["object_key"] == "docs/SecretReport.txt"));
    assert!(
        all.iter()
            .any(|v| v["object_key"] == "zero.bin" && v["total_size_bytes"] == 0)
    );
    for query in [
        [("q", "x"), ("limit", "0")],
        [("q", "x"), ("field", "bad")],
        [("q", "x"), ("scope", "../bad")],
    ] {
        assert_eq!(
            client
                .get(format!("{base}/namespace/search"))
                .bearer_auth(&token)
                .query(&query)
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
    }
    assert_eq!(
        io_before,
        resume_progress::health_submitted(&client, &base, &token).await,
        "search submitted N1 payload I/O"
    );

    drop(stack.metadata.take());
    stack.start_metadata().await;
    let resumed = page(
        &client,
        &base,
        &token,
        &[
            ("q", "REPORT"),
            ("scope", "/docs/"),
            ("limit", "1"),
            ("continuation_token", cursor.as_str()),
        ],
    )
    .await;
    assert_eq!(resumed["results"], second["results"]);
    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", stack.native))
        .await
        .unwrap();
    assert!(
        native(&mut socket, "auth.bearer", json!({"token":token}), None)
            .await
            .0
            .ok
    );
    let (response, body) = native(
        &mut socket,
        "namespace.search",
        json!({"q":"report","scope":"docs","limit":10}),
        None,
    )
    .await;
    assert!(response.ok);
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["results"].as_array().unwrap().len(), 2);

    let initial = preparation_bench::resources(stack.metadata.as_ref().unwrap().0.id());
    let mut readers = Vec::new();
    for _ in 0..4 {
        let (client, base, token) = (client.clone(), base.clone(), token.clone());
        readers.push(tokio::spawn(async move {
            let mut timings = Vec::new();
            for _ in 0..32 {
                let start = Instant::now();
                let result = page(&client, &base, &token, &[("q", "report")]).await;
                assert!(!result["results"].as_array().unwrap().is_empty());
                timings.push(start.elapsed().as_secs_f64() * 1000.0);
            }
            timings
        }));
    }
    for i in 0..16 {
        maintenance::put(
            &client,
            &base,
            &token,
            &format!("concurrent/write-{i:02}.bin"),
            &bytes[..4096],
        )
        .await;
    }
    let mut timings = Vec::new();
    for reader in readers {
        timings.extend(reader.await.unwrap());
    }
    let final_resources = preparation_bench::resources(stack.metadata.as_ref().unwrap().0.id());
    let access = stack.root.join("search-console-access.json");
    std::fs::write(&access,serde_json::to_vec_pretty(&json!({"metadata":stack.url,"fragment":fragment,"token":token,"console_port":port(),"output":stack.root})).unwrap()).unwrap();
    std::fs::set_permissions(&access, std::fs::Permissions::from_mode(0o600)).unwrap();
    let log = std::fs::File::create(stack.root.join("search-console.log")).unwrap();
    let mut child = Process(
        Command::new(std::env::var_os("N1_BROWSER_NODE").expect("browser Node required"))
            .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/noa_service/search.mjs"))
            .arg(&access)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap(),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(
                status.success(),
                "{}",
                std::fs::read_to_string(stack.root.join("search-console.log")).unwrap()
            );
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "search browser timed out"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let read = client
        .get(format!(
            "{base}/objects/get/docs/Report.TXT?version_id={version}"
        ))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(read.bytes().await.unwrap().as_ref(), bytes);
    reliability::record(
        &stack.root.join("SEARCH-PASSED.json"),
        &json!({"passed":true,"accel_required":false,"native_and_http":true,"cursor_restart":true,"fragment_isolation":true,"search_payload_io":0,"concurrent_search_latencies":preparation_bench::distribution(timings),"m1_before":initial,"m1_after":final_resources}),
    );
}
