//! Explicit repair of sealed native staging must not publish a named object.
use super::*;
use n1_link::{PoolLayout, noa::Command as ObjectCommand};

async fn object_client(stack: &Stack) -> n1_native_client::N1NativeClient {
    let credentials: Value = serde_json::from_slice(
        &std::fs::read(stack.object_config.with_extension("credentials.json")).unwrap(),
    )
    .unwrap();
    let mut client =
        n1_native_client::N1NativeClient::connect(credentials["native_url"].as_str().unwrap())
            .await
            .unwrap();
    client
        .auth_fragment(
            credentials["fragment"].as_str().unwrap(),
            credentials["secret"].as_str().unwrap(),
        )
        .await
        .unwrap();
    client
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires real isolated N1/M1 processes and two physical member roots"]
async fn explicit_native_staging_repair_preserves_m1_acks_identity_and_publication_boundary() {
    assert!(std::env::var_os("N1_OBJECT_TEST_SECOND_ROOT").is_some());
    let mut stack = Stack::new().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (alias, token) = reliability::fragment(&client, &base, "sealed staging repair").await;
    let bytes: Vec<_> = (0..25003).map(|i| (i * 31 % 251) as u8).collect();
    let part_bytes = 5001;
    let route = format!("{base}/upload/multipart/durable");
    let upload = json_response(
        client
            .post(format!("{route}/init/staged.bin"))
            .bearer_auth(&token)
            .json(&json!({"total_size_bytes":bytes.len(),
            "part_size_bytes":part_bytes,"expected_parts":5}))
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let version = upload["version_id"].as_str().unwrap();
    for (index, part) in bytes.chunks(part_bytes).enumerate() {
        resume_progress::put(
            &client,
            &route,
            &token,
            version,
            index as u64,
            part.to_vec(),
        )
        .await;
    }
    drop(stack.metadata.take());
    let database = maintenance::db(&stack);
    let fragment = m1::fragments::resolve(&database, &alias).unwrap().id;
    let pending: Value = serde_json::from_slice(
        &database
            .snapshot()
            .get(
                m1::Table::Uploads,
                &m1::RecordKey::new(fragment, version).unwrap(),
            )
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let key = pending["upload"]["object_key"].as_str().unwrap().to_owned();
    drop(database);
    // Model a crash after native durable completion but before named publication.
    let mut native = object_client(&stack).await;
    native
        .write_at(
            key.clone(),
            0,
            bytes.len() as u64,
            &mut std::io::Cursor::new(&bytes),
        )
        .await
        .unwrap();
    drop(native);
    drop(stack.object.take());
    let settings: Value =
        serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
    let layout: PoolLayout =
        serde_json::from_value(settings["pools"][0]["layout"].clone()).unwrap();
    let roots: Vec<PathBuf> =
        serde_json::from_value(settings["pools"][0]["roots"].clone()).unwrap();
    let fp = n1_link::ObjectKey::inspect(&key).unwrap().fingerprint();
    let damaged = roots[layout.member_slot(fp, 0).unwrap()].join(fp.staging_path());
    std::fs::OpenOptions::new()
        .write(true)
        .open(&damaged)
        .unwrap()
        .set_len(128)
        .unwrap();
    stack.start_object().await;
    stack.start_metadata().await;
    let state = resume_progress::status(&client, &route, &token, version).await;
    assert_eq!(state["uploaded_parts"], json!([0, 1, 2, 3, 4]));
    let finish = format!(
        "{route}/{version}/{}/finalize",
        upload["session_id"].as_str().unwrap()
    );
    assert!(
        !client
            .post(&finish)
            .bearer_auth(&token)
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    assert_eq!(
        client
            .get(format!("{base}/objects/get/staged.bin"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    let before = std::fs::read(&damaged).unwrap();
    let mut native = object_client(&stack).await;
    assert!(
        native
            .write_at(
                key.clone(),
                0,
                bytes.len() as u64,
                &mut std::io::Cursor::new(&bytes)
            )
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&damaged).unwrap(), before);
    native
        .control(ObjectCommand::ObjectRepair {
            object_key: key.clone(),
        })
        .await
        .unwrap();
    assert!(
        native
            .control(ObjectCommand::ObjectStat {
                object_key: key.clone()
            })
            .await
            .is_err()
    );
    drop(native);
    for shard in 0..layout.width() {
        assert!(
            !roots[layout.member_slot(fp, shard as u16).unwrap()]
                .join(fp.shard_path(shard as u16))
                .exists()
        );
    }
    drop(stack.metadata.take());
    drop(stack.object.take());
    stack.start_object().await;
    stack.start_metadata().await;
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
    let get = client
        .get(format!("{base}/objects/get/staged.bin"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 200);
    assert_eq!(get.bytes().await.unwrap().as_ref(), bytes);
    std::fs::write(
        stack.root.join("STAGED-REPAIR-PASSED.json"),
        serde_json::to_vec_pretty(&json!({"passed":true,"external_parts_retained":5,
            "readonly_retry_preserved":true,"repair_did_not_publish":true,
            "same_version":version,"full_readback_exact":true}))
        .unwrap(),
    )
    .unwrap();
}
