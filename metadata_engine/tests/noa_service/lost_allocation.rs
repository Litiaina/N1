//! Reproduce the permitted loss of an unused N1 Begin after durable M1 ACKs.
//! This is an explicit allocation-loss fixture, not a physical power-cut claim.
use super::*;
use m1::object_store::{NativeObjectStore, ObjectStoreConfig, ObjectUpload};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real services and removal of only this fixture's unused native shard allocations"]
async fn durable_bridge_recovers_lost_unused_allocation_without_recreating_the_old_link() {
    let mut stack = Stack::with_block_bytes(262144).await;
    let client = Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (alias, token) = reliability::fragment(&client, &base, "Lost unused allocation").await;
    let route = format!("{base}/upload/multipart/durable");
    let credentials: Value = serde_json::from_slice(
        &std::fs::read(stack.object_config.with_extension("credentials.json")).unwrap(),
    )
    .unwrap();
    let settings: Value =
        serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
    let layout: n1_link::PoolLayout =
        serde_json::from_value(settings["pools"][0]["layout"].clone()).unwrap();
    let roots: Vec<PathBuf> =
        serde_json::from_value(settings["pools"][0]["roots"].clone()).unwrap();
    for (case, length) in [0usize, 2 * 1024 * 1024 + 123].into_iter().enumerate() {
        let bytes: Vec<u8> = (0..length).map(|i| (i * 31 % 251) as u8).collect();
        let part_bytes = 150001;
        let count = length.div_ceil(part_bytes).max(1);
        let path = format!("lost-{case}.bin");
        let upload = json_response(
            client
                .post(format!("{route}/init/{path}"))
                .bearer_auth(&token)
                .json(&json!({"total_size_bytes":length,
                "part_size_bytes":part_bytes,"expected_parts":count}))
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        let version = upload["version_id"].as_str().unwrap();
        for index in 0..count {
            let start = index * part_bytes;
            let part = bytes[start..length.min(start + part_bytes)].to_vec();
            resume_progress::put(&client, &route, &token, version, index as u64, part).await;
        }
        drop(stack.metadata.take());
        drop(stack.object.take());
        let database = maintenance::db(&stack);
        let fragment = m1::fragments::resolve(&database, &alias).unwrap().id;
        let saved: Value = serde_json::from_slice(
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
        let allocation: ObjectUpload = serde_json::from_value(saved["upload"].clone()).unwrap();
        drop(database);
        let identity = n1_link::ObjectKey::inspect(&allocation.object_key).unwrap();
        let fp = identity.fingerprint();
        for shard in 0..layout.width() {
            let stage =
                roots[layout.member_slot(fp, shard as u16).unwrap()].join(fp.staging_path());
            assert_eq!(
                std::fs::metadata(&stage).unwrap().len(),
                128,
                "fixture must remove only unused Begin headers"
            );
            std::fs::remove_file(&stage).unwrap();
            std::fs::File::open(stage.parent().unwrap())
                .unwrap()
                .sync_all()
                .unwrap();
        }
        stack.start_object().await;
        stack.start_metadata().await;
        let objects = NativeObjectStore::new(ObjectStoreConfig {
            native_url: credentials["native_url"].as_str().unwrap().into(),
            fragment: credentials["fragment"].as_str().unwrap().into(),
            secret: credentials["secret"].as_str().unwrap().into(),
            max_connections: 1,
            tls: None,
        })
        .unwrap();
        assert_eq!(
            objects
                .status(&allocation, allocation.parts, 1)
                .await
                .unwrap_err()
                .status(),
            404
        );
        let state = resume_progress::status(&client, &route, &token, version).await;
        assert_eq!(
            state["uploaded_parts"],
            json!((0..count).collect::<Vec<_>>())
        );
        assert_eq!(state["durable"], true);
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
        for _ in 0..2 {
            let result = json_response(
                client
                    .post(format!(
                        "{route}/{version}/{}/finalize",
                        upload["session_id"].as_str().unwrap()
                    ))
                    .bearer_auth(&token)
                    .json(&json!({}))
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await;
            assert_eq!(result["version_id"], version);
        }
        assert_eq!(
            objects
                .status(&allocation, allocation.parts, 1)
                .await
                .unwrap_err()
                .status(),
            404
        );
        let response = client
            .get(format!("{base}/objects/get/{path}"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["etag"]
                .to_str()
                .unwrap()
                .trim_matches('"'),
            version
        );
        assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
        drop(stack.metadata.take());
        let database = maintenance::db(&stack);
        let current: Value = serde_json::from_slice(
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
        let next: ObjectUpload = serde_json::from_value(current["upload"].clone()).unwrap();
        assert_ne!(allocation.object_key, next.object_key);
        let next_identity = n1_link::ObjectKey::inspect(&next.object_key).unwrap();
        assert_eq!(identity.pool, next_identity.pool);
        assert_eq!(identity.generation, next_identity.generation);
        assert!(current["completed"].as_bool().unwrap());
        drop(database);
        stack.start_metadata().await;
    }
}
