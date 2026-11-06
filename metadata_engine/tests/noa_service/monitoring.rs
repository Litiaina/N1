use super::*;
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real two-HDD monitoring, outage and backup qualification"]
async fn monitoring_is_scoped_cached_independent_and_metadata_backup_restores_named_objects() {
    let mut stack = Stack::new().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let fragment=json_response(client.post(format!("{base}/fragments")).json(&json!({"name":"Monitoring witness","secret":"test-only-master","key":"test-only-fragment"})).send().await.unwrap(),201).await;
    let alias = fragment["fragment_hash"].as_str().unwrap();
    let login = |secret: &str| {
        client.post(format!("{base}/auth/fragment")).json(&json!({"fragment":alias,"secret":secret,"read":true,"write":true,"modify":true,"delete":true}))
    };
    let logged = json_response(login("test-only-fragment").send().await.unwrap(), 200).await;
    let token = logged["access_token"].as_str().unwrap();
    let operator = json_response(login("test-only-master").send().await.unwrap(), 200).await;
    let operator = operator["access_token"].as_str().unwrap();
    let path = "test/monitoring.bin";
    let bytes = vec![49; 131_195];
    json_response(
        client
            .post(format!("{base}/upload/one-shot/{path}"))
            .bearer_auth(token)
            .body(bytes.clone())
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
    let scoped = format!("{base}/maintenance/fragments/{alias}/health");
    let first = json_response(
        client.get(&scoped).bearer_auth(token).send().await.unwrap(),
        200,
    )
    .await;
    assert_eq!(
        first["components"]["metadata"]["status"], "healthy",
        "{first}"
    );
    assert_eq!(
        first["components"]["object"]["status"], "healthy",
        "{first}"
    );
    assert_eq!(first["components"]["metadata"]["product"], "M1");
    assert_eq!(first["components"]["metadata"]["database"], "ProcyonDB");
    assert_eq!(first["components"]["object"]["product"], "N1");
    let metadata_pid = first["components"]["metadata"]["node"]["pid"]
        .as_u64()
        .unwrap();
    let object_pid = first["components"]["object"]["node"]["pid"]
        .as_u64()
        .unwrap();
    assert_ne!(metadata_pid, object_pid);
    assert_eq!(metadata_pid, stack.metadata.as_ref().unwrap().0.id() as u64);
    assert_eq!(object_pid, stack.object.as_ref().unwrap().0.id() as u64);
    assert!(
        first["components"]["metadata"]["storage"]["database_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(first["monitoring"]["automatic_content_scan"], false);
    let mut requests = Vec::new();
    for _ in 0..64 {
        let (client, scoped, token) = (client.clone(), scoped.clone(), token.to_owned());
        requests.push(tokio::spawn(async move {
            json_response(
                client.get(scoped).bearer_auth(token).send().await.unwrap(),
                200,
            )
            .await
        }));
    }
    for request in requests {
        let value = request.await.unwrap();
        assert!(
            value["components"]["object"]["observation"]["sequence"]
                .as_u64()
                .unwrap()
                <= 2
        );
        assert_eq!(value["consistency"]["total_objects_scanned"], Value::Null);
    }
    let pools = json_response(
        client
            .get(format!("{base}/maintenance/pools/health"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(pools["architecture"], "LINK");
    let before = super::resume_progress::health_submitted(&client, &base, token).await;
    for _ in 0..8 {
        json_response(
            client.get(&scoped).bearer_auth(token).send().await.unwrap(),
            200,
        )
        .await;
    }
    assert_eq!(
        before,
        super::resume_progress::health_submitted(&client, &base, token).await,
        "ordinary monitoring submitted payload I/O"
    );
    json_response(
        client
            .get(format!("{base}/maintenance/pools/health?deep_scan=true"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let diagnostic = json_response(
            client
                .get(format!("{base}/maintenance/pools/health?deep_scan=false"))
                .bearer_auth(token)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        let integrity = &diagnostic["pools"][0]["integrity"];
        if integrity["status"] != "running" {
            assert_eq!(integrity["status"], "passed", "{diagnostic}");
            assert_eq!(integrity["versions_checked"], 1);
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let members = pools["pools"][0]["members"].as_array().unwrap();
    assert!(!members.is_empty());
    for member in members {
        assert_eq!(member["status"], "online");
        assert!(member["total_bytes"].as_u64().unwrap() > 0);
        assert!(member["available_bytes"].as_u64().unwrap() > 0);
        assert!(member["member_id"].is_string());
    }
    let different = json_response(
        client
            .post(format!("{base}/fragments"))
            .json(&json!({"name":"Other witness","secret":"test-only-master","key":"other"}))
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
    assert_eq!(
        client
            .get(format!(
                "{base}/maintenance/fragments/{}/health",
                different["fragment_hash"].as_str().unwrap()
            ))
            .bearer_auth(token)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    let backups = format!("{base}/maintenance/metadata/backups");
    assert_eq!(
        client
            .get(&backups)
            .bearer_auth(token)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    let job = json_response(
        client
            .post(&backups)
            .bearer_auth(operator)
            .send()
            .await
            .unwrap(),
        202,
    )
    .await;
    let snapshot = job["id"].as_str().unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let listed = json_response(
            client
                .get(&backups)
                .bearer_auth(operator)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        if listed["job"]["phase"] == "completed" {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "{listed}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let archive = client
        .get(format!("{backups}/{snapshot}/download"))
        .bearer_auth(operator)
        .send()
        .await
        .unwrap();
    assert_eq!(archive.status(), 200);
    let archive = archive.bytes().await.unwrap();
    let signed = json_response(
        client
            .post(format!("{backups}/{snapshot}/signed-url"))
            .bearer_auth(operator)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let url = format!("{}{}", stack.url, signed["path"].as_str().unwrap());
    let signed_archive = client.get(&url).send().await.unwrap();
    assert_eq!(signed_archive.status(), 200);
    assert_eq!(signed_archive.bytes().await.unwrap(), archive);
    assert_eq!(
        client.get(format!("{url}x")).send().await.unwrap().status(),
        401
    );
    let file = stack.root.join("downloaded.n1meta");
    std::fs::write(&file, &archive).unwrap();
    let restored = stack.root.join("restored-metadata");
    assert!(m1::MetadataEngine::restore_backup(&file, &restored).unwrap() > 0);
    drop(stack.metadata.take());
    let mut ini = ini::Ini::load_from_file(stack.root.join("m1_config.ini")).unwrap();
    ini.with_section(Some("metadata_engine"))
        .set("data_dir", restored.display().to_string());
    ini.write_to_file(stack.root.join("m1_config.ini")).unwrap();
    stack.start_metadata().await;
    let response = client
        .get(format!("{base}/objects/get/{path}"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
    // Kill only the object service: metadata remains separately observable.
    drop(stack.object.take());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(12);
    loop {
        let health = json_response(
            client.get(&scoped).bearer_auth(token).send().await.unwrap(),
            200,
        )
        .await;
        if health["components"]["object"]["status"] == "unavailable" {
            assert_eq!(health["components"]["metadata"]["status"], "healthy");
            assert_eq!(health["status"], "degraded");
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "{health}");
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    stack.start_object().await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(12);
    loop {
        let health = json_response(
            client.get(&scoped).bearer_auth(token).send().await.unwrap(),
            200,
        )
        .await;
        if health["components"]["object"]["status"] == "healthy" {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "{health}");
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}
