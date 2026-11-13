//! Decode actual ZIP64/TAR payloads from public named archive routes.
use super::*;
use futures_util::TryStreamExt;
use std::collections::BTreeMap;
use tokio_util::compat::FuturesAsyncReadCompatExt;

async fn unpack_tar(bytes: &[u8]) -> BTreeMap<String, Vec<u8>> {
    let mut archive = tokio_tar::Archive::new(bytes);
    let mut entries = archive.entries().unwrap();
    let mut out = BTreeMap::new();
    while let Some(mut entry) = entries.try_next().await.unwrap() {
        let path = entry.path().unwrap().to_string_lossy().into_owned();
        let mut data = Vec::new();
        entry.read_to_end(&mut data).await.unwrap();
        assert!(out.insert(path, data).is_none());
    }
    out
}

async fn unpack_zip(bytes: Vec<u8>) -> BTreeMap<String, Vec<u8>> {
    let archive = async_zip::base::read::mem::ZipFileReader::new(bytes)
        .await
        .unwrap();
    let mut out = BTreeMap::new();
    for index in 0..archive.file().entries().len() {
        let path = archive.file().entries()[index]
            .filename()
            .as_str()
            .unwrap()
            .to_owned();
        let mut entry = archive.reader_without_entry(index).await.unwrap().compat();
        let mut data = Vec::new();
        entry.read_to_end(&mut data).await.unwrap();
        assert!(out.insert(path, data).is_none());
    }
    out
}

async fn no_archive_residue(stack: &mut Stack) {
    drop(stack.metadata.take());
    let config = m1::server::ServerConfig::load(&stack.root.join("m1_config.ini")).unwrap();
    let db = m1::MetadataEngine::open(&config.data_dir, m1::Options::default()).unwrap();
    let catalog = n1_link::PoolId([0; 16]);
    for prefix in ["archive/", "pin-owner/"] {
        assert!(
            db.snapshot()
                .scan(m1::Table::Reclamation, catalog, prefix, None, 1000)
                .unwrap()
                .is_empty(),
            "residual {prefix}"
        );
    }
    drop(db);
    let manifests = config.staging_dir.join("exports/manifests");
    assert_eq!(std::fs::read_dir(manifests).unwrap().count(), 0);
    stack.start_metadata().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated N1 and M1 processes with physical fixture roots"]
async fn native_archives_stream_beyond_control_limit_and_finish_cleanup() {
    let mut stack = Stack::with_block_bytes(262144).await;
    let client = Client::builder()
        .timeout(Duration::from_secs(180))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token, _) = maintenance::auth(&client, &base, "native-archives").await;
    let data = (0..9 * 1024 * 1024 + 17)
        .map(|i| (i % 251) as u8)
        .collect::<Vec<_>>();
    json_response(
        client
            .post(format!("{base}/upload/one-shot/source.bin"))
            .bearer_auth(&token)
            .body(data.clone())
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
    let path = format!("nested/{}/file.bin", "星".repeat(70));
    let expected = BTreeMap::from([(path.clone(), data.clone())]);
    for format in ["tar", "zip"] {
        let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", stack.native))
            .await
            .unwrap();
        assert!(
            native(&mut socket, "auth.bearer", json!({"token":token}), None)
                .await
                .0
                .ok
        );
        let started = std::time::Instant::now();
        let (response, bytes) = native(
            &mut socket,
            "object.archive",
            json!({"entries":[{"object_key":"source.bin","archive_path":path}],"format":format}),
            None,
        )
        .await;
        assert!(response.ok);
        assert_eq!(response.status, Some(200));
        assert!(bytes.len() > 8 * 1024 * 1024);
        assert_eq!(response.body_len, Some(bytes.len() as u64));
        let elapsed = started.elapsed().as_secs_f64();
        reliability::record(
            &stack.root.join(format!("native-archive-{format}.json")),
            &json!({"format":format,"source_bytes":data.len(),"archive_bytes":bytes.len(),
                "stream_seconds":elapsed,"stream_mib_s":bytes.len() as f64 / 1048576.0 / elapsed,
                "includes_capture_verification_and_cleanup":true}),
        );
        let actual = if format == "tar" {
            unpack_tar(&bytes).await
        } else {
            unpack_zip(bytes).await
        };
        assert_eq!(actual, expected);
        // The same connection remains aligned after the streamed body.
        assert!(
            native(
                &mut socket,
                "namespace.stat",
                json!({"path":"source.bin"}),
                None
            )
            .await
            .0
            .ok
        );
        drop(socket);
        // A completed body must have released pins and durable cleanup intents,
        // even if M1 is killed immediately instead of being allowed idle time.
        no_archive_residue(&mut stack).await;
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
    let (response, bytes) = native(
        &mut socket,
        "object.archive_manifest",
        json!({"format":"tar"}),
        Some(b"source.bin\n"),
    )
    .await;
    assert_eq!(response.status, Some(200));
    assert_eq!(response.body_len, Some(bytes.len() as u64));
    assert_eq!(
        unpack_tar(&bytes).await,
        BTreeMap::from([("source.bin".into(), data)])
    );
    drop(socket);
    no_archive_residue(&mut stack).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated N1 and M1 processes with physical fixture roots"]
async fn selected_and_manifest_archives_preserve_versions_formats_permissions_and_cleanup() {
    let mut stack = Stack::new().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(180))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (alias, token, _) = maintenance::auth(&client, &base, "archives").await;
    let first = (0..131091).map(|n| (n % 251) as u8).collect::<Vec<_>>();
    let second = b"another immutable source".to_vec();
    let old = json_response(
        client
            .post(format!("{base}/upload/one-shot/folder/a.bin"))
            .bearer_auth(&token)
            .body(first.clone())
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
    json_response(
        client
            .post(format!("{base}/upload/one-shot/folder/b.bin"))
            .bearer_auth(&token)
            .body(second.clone())
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
    json_response(
        client
            .post(format!("{base}/upload/one-shot/empty.bin"))
            .bearer_auth(&token)
            .body(Vec::new())
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
    let expected = BTreeMap::from([
        ("folder/a.bin".into(), first.clone()),
        ("folder/b.bin".into(), second.clone()),
        ("empty.bin".into(), Vec::new()),
    ]);
    for format in ["zip", "tar"] {
        let response = client.post(format!("{base}/objects/archive"))
            .bearer_auth(&token).json(&json!({"keys":["folder/a.bin","folder/b.bin","empty.bin"],"format":format,"filename":"witness"})).send().await.unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.headers()["content-type"],
            if format == "zip" {
                "application/zip"
            } else {
                "application/x-tar"
            }
        );
        let bytes = response.bytes().await.unwrap();
        let actual = if format == "zip" {
            unpack_zip(bytes.to_vec()).await
        } else {
            unpack_tar(&bytes).await
        };
        assert_eq!(actual, expected);
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
    let (response, bytes) = native(
        &mut socket,
        "object.archive",
        json!({"keys":["empty.bin"],"format":"tar"}),
        None,
    )
    .await;
    assert_eq!(response.status, Some(200));
    assert_eq!(
        unpack_tar(&bytes).await,
        BTreeMap::from([("empty.bin".into(), Vec::new())])
    );
    drop(socket);
    let response = client
        .post(format!("{base}/objects/archive"))
        .bearer_auth(&token)
        .json(&json!({"prefix":"folder","format":"zip"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.headers()["content-type"], "application/x-tar");
    assert_eq!(
        unpack_tar(&response.bytes().await.unwrap()).await,
        BTreeMap::from([
            ("folder/a.bin".into(), first.clone()),
            ("folder/b.bin".into(), second.clone())
        ])
    );

    // A line can pin the older version after a newer head has been published.
    json_response(
        client
            .post(format!("{base}/upload/one-shot/folder/a.bin"))
            .bearer_auth(&token)
            .body(b"replacement".to_vec())
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
    let inventory = format!(
        "{}\tfolder/a.bin\nfolder/b.bin\n\n",
        old["version_id"].as_str().unwrap()
    );
    let response = client
        .post(format!(
            "{base}/objects/archive/manifest?format=tar&filename=inventory"
        ))
        .bearer_auth(&token)
        .body(inventory)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        unpack_tar(&response.bytes().await.unwrap()).await,
        BTreeMap::from([
            ("folder/a.bin".into(), first),
            ("folder/b.bin".into(), second)
        ])
    );
    let write_only = json_response(
        client
            .post(format!("{base}/auth/fragment"))
            .json(&json!({"fragment":alias,"secret":"test-only-fragment","write":true}))
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let denied = client
        .post(format!("{base}/objects/archive"))
        .bearer_auth(write_only["access_token"].as_str().unwrap())
        .json(&json!({"keys":["folder/a.bin"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 403);
    let oversized = client
        .post(format!("{base}/objects/archive"))
        .bearer_auth(&token)
        .header("content-type", "application/json")
        .body(vec![b' '; 4 * 1024 * 1024 + 1])
        .send()
        .await
        .unwrap();
    assert_eq!(oversized.status(), 413);
    let read_only = json_response(
        client
            .post(format!("{base}/auth/fragment"))
            .json(&json!({"fragment":alias,"secret":"test-only-fragment","read":true}))
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", stack.native))
        .await
        .unwrap();
    assert!(
        native(
            &mut socket,
            "auth.bearer",
            json!({"token":read_only["access_token"]}),
            None
        )
        .await
        .0
        .ok
    );
    let (response, bytes) = native(
        &mut socket,
        "object.archive_manifest",
        json!({"format":"tar"}),
        Some(b"empty.bin\n"),
    )
    .await;
    assert_eq!(response.status, Some(200));
    assert_eq!(
        unpack_tar(&bytes).await,
        BTreeMap::from([("empty.bin".into(), Vec::new())])
    );
    drop(socket);
    for (input, expected) in [
        ("../escape".to_owned(), 400),
        ("x".repeat(8193), 400),
        ("missing.bin".to_owned(), 404),
    ] {
        let response = client
            .post(format!("{base}/objects/archive/manifest"))
            .bearer_auth(&token)
            .body(input)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
    }
    // Startup drains incomplete archive intents, partial manifests and pins.
    // Wait for live workers' terminal cleanup, then restart and inspect the DB.
    tokio::time::sleep(Duration::from_millis(500)).await;
    no_archive_residue(&mut stack).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated N1 and M1 processes with physical fixture roots"]
async fn archive_disconnect_crash_and_empty_source_loss_never_leave_success_or_pins() {
    let mut stack = Stack::new().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(180))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token, _) = maintenance::auth(&client, &base, "archive-faults").await;
    maintenance::put(
        &client,
        &base,
        &token,
        "source.bin",
        &vec![0x71; 1024 * 1024],
    )
    .await;
    maintenance::put(&client, &base, &token, "empty.bin", &[]).await;
    let inventory = "source.bin\n".repeat(128);
    for crash in [false, true] {
        let mut response = client
            .post(format!("{base}/objects/archive/manifest"))
            .bearer_auth(&token)
            .body(inventory.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert!(!response.chunk().await.unwrap().unwrap().is_empty());
        if crash {
            // Kill the actual M1 while an archive and immutable source pins are live.
            drop(stack.metadata.take());
            {
                let db = maintenance::db(&stack);
                let view = db.snapshot();
                for prefix in ["archive/", "pin-owner/"] {
                    assert!(
                        !view
                            .scan(
                                m1::Table::Reclamation,
                                n1_link::PoolId([0; 16]),
                                prefix,
                                None,
                                1000
                            )
                            .unwrap()
                            .is_empty(),
                        "expected durable {prefix} before recovery"
                    );
                }
            }
            stack.start_metadata().await;
        }
        drop(response);
        tokio::time::sleep(Duration::from_secs(1)).await;
        no_archive_residue(&mut stack).await;
    }

    // Disconnect before a manifest body is complete, including while its
    // background cleanup intent may still be committing.
    let address = stack.url.strip_prefix("http://").unwrap();
    for _ in 0..2 {
        let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
        socket.write_all(format!("POST /noa/v1/objects/archive/manifest HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {token}\r\nContent-Length: 1024\r\n\r\nsource.bin\n").as_bytes()).await.unwrap();
        socket.shutdown().await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    no_archive_residue(&mut stack).await;

    // Remove only the empty object's N1 payload, leaving its M1 version intact.
    // A zero-size TAR header must not turn a missing backend into valid success.
    drop(stack.metadata.take());
    let key = {
        let db = maintenance::db(&stack);
        let fragments = db
            .snapshot()
            .scan(
                m1::Table::Reclamation,
                n1_link::PoolId([0; 16]),
                "fragment/",
                None,
                100,
            )
            .unwrap();
        let value: Value = serde_json::from_slice(&fragments[0].1).unwrap();
        let fragment = serde_json::from_value(value["id"].clone()).unwrap();
        let rows = db
            .snapshot()
            .scan(m1::Table::Versions, fragment, "", None, 100)
            .unwrap();
        rows.into_iter()
            .filter_map(|(_, bytes)| {
                let value: Value = serde_json::from_slice(&bytes).unwrap();
                (value["metadata"]["total_size_bytes"] == 0)
                    .then(|| value["object_key"].as_str().unwrap().to_owned())
            })
            .next()
            .expect("empty immutable version")
    };
    let credentials: Value = serde_json::from_slice(
        &std::fs::read(stack.object_config.with_extension("credentials.json")).unwrap(),
    )
    .unwrap();
    let mut objects =
        n1_native_client::N1NativeClient::connect(credentials["native_url"].as_str().unwrap())
            .await
            .unwrap();
    objects
        .auth_fragment(
            credentials["fragment"].as_str().unwrap(),
            credentials["secret"].as_str().unwrap(),
        )
        .await
        .unwrap();
    objects
        .control(n1_link::noa::Command::ObjectDelete { object_key: key })
        .await
        .unwrap();
    stack.start_metadata().await;
    for format in ["tar", "zip"] {
        let response = client
            .post(format!("{base}/objects/archive"))
            .bearer_auth(&token)
            .json(&json!({"keys":["empty.bin"],"format":format}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert!(
            response.bytes().await.is_err(),
            "missing backend accepted as a complete {format}"
        );
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    no_archive_residue(&mut stack).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real graceful shutdown and immutable export-pin recovery on private physical roots"]
async fn graceful_export_shutdown_keeps_the_captured_manifest_pinned_until_restart() {
    let mut stack = Stack::new().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(180))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token, _) = maintenance::auth(&client, &base, "export-stop").await;
    maintenance::put(
        &client,
        &base,
        &token,
        "source.bin",
        &vec![0x63; 1024 * 1024],
    )
    .await;
    let entries: Vec<_> = (0..256).map(|index|json!({"object_key":"source.bin","archive_path":format!("copy-{index:03}.bin")})).collect();
    let created = json_response(
        client
            .post(format!("{base}/exports"))
            .bearer_auth(&token)
            .json(&json!({"entries":entries,"format":"tar"}))
            .send()
            .await
            .unwrap(),
        202,
    )
    .await;
    let id = created["export_id"].as_str().unwrap();
    let status_url = format!("{base}/exports/{id}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let state = json_response(
            client
                .get(&status_url)
                .bearer_auth(&token)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        if state["phase"] == "building" {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "export never entered building: {state}"
        );
        assert_ne!(
            state["phase"], "ready",
            "workload completed before the shutdown injection"
        );
        assert_ne!(state["phase"], "failed", "{state}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut process = stack.metadata.take().unwrap();
    assert_eq!(
        unsafe { libc::kill(process.0.id() as i32, libc::SIGTERM) },
        0
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(status) = process.0.try_wait().unwrap() {
            assert!(status.success(), "graceful shutdown failed: {status}");
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "graceful shutdown did not drain"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    {
        let db = maintenance::db(&stack);
        let view = db.snapshot();
        let catalog = n1_link::PoolId([0; 16]);
        let record: Value = serde_json::from_slice(
            &view
                .get(
                    m1::Table::Reclamation,
                    &m1::RecordKey::new(catalog, format!("export/{id}")).unwrap(),
                )
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(record["phase"], "queued");
        assert_eq!(record["manifest_ready"], true);
        assert!(
            !view
                .scan(
                    m1::Table::Reclamation,
                    catalog,
                    &format!("pin-owner/{id}/"),
                    None,
                    100
                )
                .unwrap()
                .is_empty()
        );
    }
    stack.start_metadata().await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
    loop {
        let state = json_response(
            client
                .get(&status_url)
                .bearer_auth(&token)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        if state["phase"] == "ready" {
            assert_eq!(state["objects_completed"], 256);
            break;
        }
        assert_ne!(state["phase"], "failed", "{state}");
        assert!(
            tokio::time::Instant::now() < deadline,
            "export resume timed out: {state}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let response = client
        .get(format!("{status_url}/download"))
        .bearer_auth(&token)
        .header("range", "bytes=0-1023")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 206);
    let bytes = response.bytes().await.unwrap();
    assert_eq!(bytes.len(), 1024);
    assert_eq!(&bytes[512..], &[0x63; 512]);
}
