//! Compare full reconstruction and indexed resume of the same actual HDD data.
//! The object reserves a 15 GiB identity; exactly 1 GiB is uploaded, not 15 GiB.
use super::resume_progress::{health_submitted, put, status};
use super::*;

async fn measured_status(
    client: &Client,
    base: &str,
    route: &str,
    token: &str,
    version: &str,
) -> Value {
    let before = health_submitted(client, base, token).await;
    let start = std::time::Instant::now();
    let result = status(client, route, token, version).await;
    let seconds = start.elapsed().as_secs_f64();
    let after = health_submitted(client, base, token).await;
    assert_eq!(
        result["uploaded_parts"],
        json!((0..128).collect::<Vec<_>>())
    );
    assert_eq!(result["expected_parts"], 1920);
    json!({"seconds":seconds,"native_io_submissions":after-before,"acknowledged_parts":128,"acknowledged_bytes":1_u64<<30})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit object binary, SSD control root and two HDD roots; uploads 1 GiB"]
async fn large_resume_uses_metadata_pages_and_submits_no_payload_reads_even_after_process_restart()
{
    let mut stack = Stack::with_block_bytes(262144).await;
    let client = Client::builder()
        .timeout(Duration::from_secs(600))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let created=json_response(client.post(format!("{base}/fragments")).json(&json!({"name":"Large resume","secret":"test-only-master","key":"test-only-fragment"})).send().await.unwrap(),201).await;
    let logged=json_response(client.post(format!("{base}/auth/fragment")).json(&json!({"fragment":created["fragment_hash"],"secret":"test-only-fragment","read":true,"write":true})).send().await.unwrap(),200).await;
    let token = logged["access_token"].as_str().unwrap();
    let route = format!("{base}/upload/multipart");
    let shape =
        json!({"total_size_bytes":15_u64<<30,"part_size_bytes":8<<20,"expected_parts":1920});
    let upload = json_response(
        client
            .post(format!("{route}/init/large?durability=strict"))
            .bearer_auth(token)
            .json(&shape)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let version = upload["version_id"].as_str().unwrap();
    let start = std::time::Instant::now();
    for index in 0..128 {
        put(
            &client,
            &route,
            token,
            version,
            index,
            vec![(index % 251) as u8; 8 << 20],
        )
        .await;
    }
    let fixture_upload_seconds = start.elapsed().as_secs_f64();
    let warm = measured_status(&client, &base, &route, token, version).await;
    assert_eq!(warm["native_io_submissions"], 0);
    // Reconstruct the same upload's index from actual frames once, using the
    // existing fallback for an upload without acknowledged metadata coverage.
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
    let key =
        n1_metadata_engine::RecordKey::new(fragment.id, format!("upload-plan/{version}")).unwrap();
    db.transact(64, |tx| {
        let mut saved: Value = serde_json::from_slice(
            &tx.get(n1_metadata_engine::Table::Reclamation, &key)?
                .unwrap(),
        )
        .unwrap();
        saved["acknowledgements_indexed"] = json!(false);
        tx.put(
            n1_metadata_engine::Table::Reclamation,
            &key,
            &serde_json::to_vec(&saved).unwrap(),
        )
    })
    .unwrap();
    drop(db);
    stack.start_metadata().await;
    let reconstruction = measured_status(&client, &base, &route, token, version).await;
    assert!(reconstruction["native_io_submissions"].as_u64().unwrap() > 0);
    let rebuilt = measured_status(&client, &base, &route, token, version).await;
    assert_eq!(rebuilt["native_io_submissions"], 0);
    drop(stack.metadata.take());
    drop(stack.object.take());
    stack.start_object().await;
    stack.start_metadata().await;
    let cold = measured_status(&client, &base, &route, token, version).await;
    assert_eq!(cold["native_io_submissions"], 0);
    // Model uncertain native completion: valid frames exist, but their metadata
    // ACK was never committed. Reconstruction must not invent a durable ACK.
    drop(stack.metadata.take());
    let db = n1_metadata_engine::MetadataEngine::open(
        &config.data_dir,
        n1_metadata_engine::Options::default(),
    )
    .unwrap();
    let progress_key = n1_metadata_engine::RecordKey::new(
        fragment.id,
        format!("upload-progress/{version}/000000"),
    )
    .unwrap();
    db.transact(64, |tx| {
        let mut bits = tx
            .get(n1_metadata_engine::Table::Reclamation, &progress_key)?
            .unwrap();
        bits[15] &= !(1 << 7);
        tx.put(n1_metadata_engine::Table::Reclamation, &progress_key, &bits)?;
        let mut saved: Value = serde_json::from_slice(
            &tx.get(n1_metadata_engine::Table::Reclamation, &key)?
                .unwrap(),
        )
        .unwrap();
        saved["acknowledgements_indexed"] = json!(false);
        tx.put(
            n1_metadata_engine::Table::Reclamation,
            &key,
            &serde_json::to_vec(&saved).unwrap(),
        )
    })
    .unwrap();
    drop(db);
    stack.start_metadata().await;
    assert_eq!(
        status(&client, &route, token, version).await["uploaded_parts"],
        json!((0..127).collect::<Vec<_>>())
    );
    put(&client, &route, token, version, 127, vec![127; 8 << 20]).await;
    assert_eq!(
        status(&client, &route, token, version).await["uploaded_parts"],
        json!((0..128).collect::<Vec<_>>())
    );
    let report = json!({"passed":true,"reserved_object_bytes":15_u64<<30,"uploaded_fixture_bytes":1_u64<<30,"part_bytes":8<<20,"parts_uploaded":128,"durability":"strict","fixture_upload_seconds":fixture_upload_seconds,"warm_indexed":warm,"full_reconstruction":reconstruction,"rebuilt_index":rebuilt,"both_engines_restarted":cold,"uncertain_native_completion_was_retransmitted":true,"object_fully_uploaded":false,"test_roots":stack.root});
    std::fs::write(
        stack.root.join("resume-scale.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    eprintln!("{}", serde_json::to_string(&report).unwrap());
}
