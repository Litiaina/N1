//! ACK history never replaces native validation or prevents staged-data recovery.
use super::resume_progress::{put, status};
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit object binary and isolated storage roots"]
async fn native_finalize_failure_reconciles_lost_staging_and_cancel_removes_acknowledgements() {
    let mut stack = Stack::new().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let created=json_response(client.post(format!("{base}/fragments")).json(&json!({"name":"Staged loss","secret":"test-only-master","key":"test-only-fragment"})).send().await.unwrap(),201).await;
    let logged=json_response(client.post(format!("{base}/auth/fragment")).json(&json!({"fragment":created["fragment_hash"],"secret":"test-only-fragment","read":true,"write":true,"delete":true})).send().await.unwrap(),200).await;
    let token = logged["access_token"].as_str().unwrap();
    let route = format!("{base}/upload/multipart/durable");
    let shape = json!({"total_size_bytes":16384,"part_size_bytes":8192,"expected_parts":2});
    let upload = json_response(
        client
            .post(format!("{route}/init/lost-stage"))
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
    put(&client, &route, token, version, 0, vec![42; 8192]).await;
    drop(stack.metadata.take());
    drop(stack.object.take());
    let config: Value =
        serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
    let member = PathBuf::from(config["pools"][0]["roots"][0].as_str().unwrap());
    let stages = std::fs::read_dir(member.join(".uploads"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect::<Vec<_>>();
    assert_eq!(stages.len(), 1);
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(stages[0].join("shard.tmp"))
        .unwrap();
    file.set_len(128).unwrap();
    file.sync_all().unwrap();
    drop(file);
    stack.start_object().await;
    stack.start_metadata().await;
    assert_eq!(
        status(&client, &route, token, version).await["uploaded_parts"],
        json!([0])
    );
    put(&client, &route, token, version, 1, vec![43; 8192]).await;
    let finalize = format!("{route}/{version}/{session}/finalize");
    let failed = client
        .post(&finalize)
        .bearer_auth(token)
        .json(&shape)
        .send()
        .await
        .unwrap();
    assert_eq!(
        failed.status(),
        409,
        "lost payload must not be published from ACK bits"
    );
    assert_eq!(
        status(&client, &route, token, version).await["uploaded_parts"],
        json!([1]),
        "failed native validation must invalidate the fast index"
    );
    put(&client, &route, token, version, 0, vec![42; 8192]).await;
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
    let read = client
        .get(format!("{base}/objects/get/lost-stage"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(read.status(), 200);
    assert_eq!(
        read.bytes().await.unwrap().as_ref(),
        [vec![42; 8192], vec![43; 8192]].concat()
    );
    let cancelled = json_response(
        client
            .post(format!("{route}/init/cancel-stage"))
            .bearer_auth(token)
            .json(&shape)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let cancelled_version = cancelled["version_id"].as_str().unwrap();
    put(&client, &route, token, cancelled_version, 0, vec![44; 8192]).await;
    json_response(
        client
            .delete(format!("{route}/{cancelled_version}"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(
        client
            .get(format!("{route}/{cancelled_version}"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap()
            .status(),
        410
    );
    drop(stack.metadata.take());
    let config =
        n1_metadata_engine::server::ServerConfig::load(&stack.root.join("n1_config.ini")).unwrap();
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
                &format!("upload-progress/{cancelled_version}/"),
                None,
                1000
            )
            .unwrap()
            .is_empty()
    );
}
