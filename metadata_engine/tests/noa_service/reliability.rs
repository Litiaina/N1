//! Destructive faults target only newly-created fixture directories. External
//! client oracles and immutable backup archives survive all injected failures.
use super::*;
use m1::{MetadataEngine, Options, RecordKey, Table};
use n1_link::{PoolId, noa::Command as ObjectCommand};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, io::Write, os::unix::fs::OpenOptionsExt};

pub(super) fn record(path: &Path, value: &impl serde::Serialize) {
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(&mut file, value).unwrap();
    file.write_all(b"\n").unwrap();
    file.sync_all().unwrap();
}

pub(super) async fn fragment(client: &Client, base: &str, name: &str) -> (String, String) {
    let created = json_response(
        client
            .post(format!("{base}/fragments"))
            .json(&json!({
                "name":name,"secret":"test-only-master","key":"test-only-fragment"
            }))
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
    let alias = created["fragment_hash"].as_str().unwrap().to_owned();
    let logged = json_response(
        client
            .post(format!("{base}/auth/fragment"))
            .json(&json!({
                "fragment": alias,
                "secret": "test-only-fragment",
                "read": true,
                "write": true,
                "modify": true,
                "delete": true
            }))
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    (alias, logged["access_token"].as_str().unwrap().to_owned())
}

async fn direct(stack: &Stack) -> n1_native_client::N1NativeClient {
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

type Records = BTreeMap<(usize, [u8; 16], String), Vec<u8>>;
fn records(db: &MetadataEngine, scopes: &[PoolId]) -> Records {
    let snapshot = db.snapshot();
    let mut records = BTreeMap::new();
    for (index, table) in [
        Table::Entries,
        Table::Versions,
        Table::Uploads,
        Table::Reclamation,
    ]
    .into_iter()
    .enumerate()
    {
        for scope in scopes {
            let mut after: Option<RecordKey> = None;
            loop {
                let page = snapshot
                    .scan(table, *scope, "", after.as_ref(), 1000)
                    .unwrap();
                if page.is_empty() {
                    break;
                }
                after = page.last().map(|row| row.0.clone());
                for (key, value) in page {
                    assert!(
                        records
                            .insert((index, scope.0, key.identifier().into()), value)
                            .is_none()
                    );
                }
            }
        }
    }
    records
}

#[derive(serde::Serialize)]
struct Acknowledged {
    alias: String,
    path: String,
    version: String,
    sha256: String,
    #[serde(skip)]
    bytes: Vec<u8>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "destructive private two-HDD fixture: full metadata/staging loss, exact snapshot and native independence"]
async fn full_metadata_loss_restores_fragments_versions_trash_permissions_and_durable_resume() {
    let mut stack = Stack::new().await;
    let settings: Value =
        serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
    // This witness removes all M1 staging. Its unfinished parts must therefore
    // be native-aligned and backed by N1, as in the original 1+1 fixture.
    let part_bytes = settings["pools"][0]["layout"]["data_shards"]
        .as_u64()
        .unwrap() as usize
        * 8192;
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let mut identities = Vec::new();
    let mut acknowledged = Vec::new();
    let mut partials = Vec::new();
    let mut scopes = vec![PoolId([0; 16])];
    for group in 0..2 {
        let (alias, token) = fragment(&client, &base, &format!("Disaster recovery {group}")).await;
        for index in 0..12 {
            let path = format!("original/object-{index:02}.bin");
            for revision in 0..2 {
                let size = [0, 1, 127, 8193, 65537, 262147][index % 6];
                let bytes: Vec<_> = (0..size)
                    .map(|offset| ((offset * 17 + index * 31 + revision * 73 + group) % 251) as u8)
                    .collect();
                let response = json_response(
                    client
                        .post(format!("{base}/upload/one-shot/{path}?durability=strict"))
                        .bearer_auth(&token)
                        .body(bytes.clone())
                        .send()
                        .await
                        .unwrap(),
                    201,
                )
                .await;
                acknowledged.push(Acknowledged {
                    alias: alias.clone(),
                    path: path.clone(),
                    version: response["version_id"].as_str().unwrap().into(),
                    sha256: hex::encode(Sha256::digest(&bytes)),
                    bytes,
                });
            }
        }
        json_response(
            client
                .post(format!("{base}/posix/rename"))
                .bearer_auth(&token)
                .json(&json!({"source_path":"original/","destination_path":"renamed/"}))
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        for row in acknowledged.iter_mut().filter(|row| row.alias == alias) {
            row.path = row.path.replacen("original/", "renamed/", 1);
        }
        let trash = json_response(
            client
                .delete(format!("{base}/objects/renamed/object-00.bin"))
                .bearer_auth(&token)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        let shape = json!({"total_size_bytes":part_bytes*3+17,"part_size_bytes":part_bytes,"expected_parts":4});
        let route = format!("{base}/upload/multipart/durable");
        let upload = json_response(
            client
                .post(format!("{route}/init/sparse.bin?durability=strict"))
                .bearer_auth(&token)
                .json(&shape)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        for part in [0, 2] {
            super::resume_progress::put(
                &client,
                &route,
                &token,
                upload["version_id"].as_str().unwrap(),
                part,
                vec![41 + part as u8; part_bytes],
            )
            .await;
        }
        partials.push((upload, shape, trash));
        identities.push((alias, token));
    }
    record(&stack.root.join("acknowledged.json"), &acknowledged);
    // Close every metadata owner before taking a complete, stable backup. No
    // acknowledged operation after this point is claimed to be in the snapshot.
    drop(stack.metadata.take());
    let config = m1::server::ServerConfig::load(&stack.root.join("m1_config.ini")).unwrap();
    let db = MetadataEngine::open(&config.data_dir, Options::default()).unwrap();
    for (alias, _) in &identities {
        scopes.push(m1::fragments::resolve(&db, alias).unwrap().id);
    }
    let before = records(&db, &scopes);
    let mut native_keys = BTreeMap::new();
    for (group, scope) in scopes[1..].iter().enumerate() {
        for (_, bytes) in db
            .snapshot()
            .scan(Table::Versions, *scope, "", None, 1000)
            .unwrap()
        {
            let row: Value = serde_json::from_slice(&bytes).unwrap();
            if let (Some(key), Some(version)) = (
                row["object_key"].as_str(),
                row["metadata"]["version_id"].as_str(),
            ) {
                native_keys.insert(
                    (identities[group].0.clone(), version.to_owned()),
                    key.to_owned(),
                );
            }
        }
    }
    assert_eq!(native_keys.len(), acknowledged.len());
    let archive = stack.root.join("disaster.n1meta");
    let mut output = std::fs::File::create(&archive).unwrap();
    let archived_records = db.backup(&mut output).unwrap();
    assert_eq!(
        archived_records,
        before.len() as u64,
        "snapshot includes an unaccounted scope"
    );
    output.sync_all().unwrap();
    drop(output);
    drop(db);
    let backup = std::fs::read(&archive).unwrap();
    record(
        &stack.root.join("backup-oracle.json"),
        &json!({
            "records": archived_records,
            "sha256": hex::encode(Sha256::digest(&backup)),
            "acknowledged_versions": acknowledged.len(),
            "scopes": scopes.len()
        }),
    );
    // The original database and metadata staging disappear from their real
    // configured locations. Neither is used as the recovery source.
    std::fs::rename(&config.data_dir, stack.root.join("lost-database-evidence")).unwrap();
    std::fs::rename(
        &config.staging_dir,
        stack.root.join("lost-staging-evidence"),
    )
    .unwrap();
    assert!(!config.data_dir.exists() && !config.staging_dir.exists());
    let mut native = direct(&stack).await;
    for row in &acknowledged {
        let mut bytes = Vec::new();
        native
            .download(
                native_keys[&(row.alias.clone(), row.version.clone())].clone(),
                0,
                None,
                &mut bytes,
            )
            .await
            .unwrap();
        assert_eq!(
            bytes, row.bytes,
            "metadata loss damaged native object bytes"
        );
    }
    drop(native);
    // Every damaged archive must fail without publishing a usable destination.
    for (index, cut) in [0, backup.len() / 2, backup.len() - 1, backup.len()]
        .into_iter()
        .enumerate()
    {
        let mut bytes = backup.clone();
        if cut == bytes.len() {
            bytes.truncate(bytes.len() - 17);
        } else {
            bytes[cut] ^= 1;
        }
        let bad = stack.root.join(format!("corrupt-{index}.n1meta"));
        std::fs::write(&bad, bytes).unwrap();
        let target = stack.root.join(format!("refused-restore-{index}"));
        assert!(MetadataEngine::restore_backup(&bad, &target).is_err());
        assert!(!target.exists(), "corrupt backup was published");
    }
    assert_eq!(
        MetadataEngine::restore_backup(&archive, &config.data_dir).unwrap(),
        archived_records
    );
    assert!(MetadataEngine::restore_backup(&archive, &config.data_dir).is_err());
    let restored = MetadataEngine::open(&config.data_dir, Options::default()).unwrap();
    assert_eq!(
        records(&restored, &scopes),
        before,
        "restored snapshot changed an application record"
    );
    drop(restored);
    // Include a cold object process too; no in-memory decoder/progress cache can
    // substitute for the durable snapshot or the surviving object shards.
    drop(stack.object.take());
    stack.start_object().await;
    stack.start_metadata().await;
    for (group, (alias, token)) in identities.iter().enumerate() {
        let (upload, shape, trash) = &partials[group];
        assert_eq!(
            client
                .get(format!("{base}/objects/get/renamed/object-00.bin"))
                .bearer_auth(token)
                .send()
                .await
                .unwrap()
                .status(),
            404
        );
        json_response(client.post(format!("{base}/posix/recover")).bearer_auth(token)
            .json(&json!({"path":"renamed/object-00.bin","trash_entry_id":trash["trash_entry_id"]}))
            .send().await.unwrap(), 200).await;
        let logged = json_response(
            client
                .post(format!("{base}/auth/fragment"))
                .json(&json!({"fragment":alias,"secret":"test-only-fragment","read":true}))
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        assert_eq!(
            client
                .post(format!("{base}/upload/one-shot/forbidden"))
                .bearer_auth(logged["access_token"].as_str().unwrap())
                .body("forbidden")
                .send()
                .await
                .unwrap()
                .status(),
            403
        );
        assert_eq!(
            client
                .post(format!("{base}/auth/fragment"))
                .json(&json!({"fragment":alias,"secret":"wrong","read":true}))
                .send()
                .await
                .unwrap()
                .status(),
            401
        );
        for row in acknowledged.iter().filter(|row| row.alias == *alias) {
            let response = client
                .get(format!("{base}/objects/get/{}", row.path))
                .query(&[("version_id", &row.version)])
                .bearer_auth(token)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            assert_eq!(
                response.headers()["etag"]
                    .to_str()
                    .unwrap()
                    .trim_matches('"'),
                row.version
            );
            assert_eq!(response.bytes().await.unwrap().as_ref(), row.bytes);
        }
        let route = format!("{base}/upload/multipart/durable");
        let version = upload["version_id"].as_str().unwrap();
        assert_eq!(
            super::resume_progress::status(&client, &route, token, version).await["uploaded_parts"],
            json!([0, 2])
        );
        for part in [1, 3] {
            super::resume_progress::put(
                &client,
                &route,
                token,
                version,
                part,
                vec![41 + part as u8; if part == 3 { 17 } else { part_bytes }],
            )
            .await;
        }
        for _ in 0..2 {
            json_response(
                client
                    .post(format!(
                        "{route}/{version}/{}/finalize",
                        upload["session_id"].as_str().unwrap()
                    ))
                    .bearer_auth(token)
                    .json(shape)
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await;
        }
        let response = client
            .get(format!("{base}/objects/get/sparse.bin"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.bytes().await.unwrap().as_ref(),
            [
                vec![41; part_bytes],
                vec![42; part_bytes],
                vec![43; part_bytes],
                vec![44; 17]
            ]
            .concat()
        );
    }
    record(
        &stack.root.join("METADATA-LOSS-PASSED.json"),
        &json!({
            "passed": true,
            "original_database_and_staging_unavailable": true,
            "native_reads_without_metadata": acknowledged.len(),
            "all_snapshot_records_identical": true,
            "fragments": identities.len(),
            "versions_verified": acknowledged.len(),
            "corrupt_archives_refused": 4,
            "trash_rename_permissions_verified": true,
            "durable_uploads_resumed": partials.len(),
            "original_snapshot_sha256": hex::encode(Sha256::digest(&backup))
        }),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "private two-HDD process witness: unavailable members, corruption and beyond-parity failure"]
async fn each_physical_member_loss_and_corruption_preserves_named_bytes_or_returns_failure() {
    let mut stack = Stack::new().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token) = fragment(&client, &base, "Member fault matrix").await;
    let mut settings: Value =
        serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
    // Keep the fault set stable until the explicit repair assertion. Automatic
    // read repair is exercised by the retained dedicated recovery tests.
    drop(stack.metadata.take());
    drop(stack.object.take());
    settings["read_repair"] = json!(false);
    std::fs::write(
        &stack.object_config,
        serde_json::to_vec_pretty(&settings).unwrap(),
    )
    .unwrap();
    stack.start_object().await;
    stack.start_metadata().await;
    let roots: Vec<PathBuf> = settings["pools"][0]["roots"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| PathBuf::from(r.as_str().unwrap()))
        .collect();
    assert_eq!(roots.len(), 2);
    use std::os::unix::fs::MetadataExt;
    assert_ne!(
        roots[0].metadata().unwrap().dev(),
        roots[1].metadata().unwrap().dev(),
        "two physical filesystems required"
    );
    let mut objects = Vec::new();
    for (index, size) in [0, 1, 4095, 4096, 4097, 65536, 65537, 2097169]
        .into_iter()
        .enumerate()
    {
        let path = format!("fault/object-{index}.bin");
        let bytes: Vec<_> = (0..size)
            .map(|n| ((n * 19 + index * 31) % 251) as u8)
            .collect();
        let ack = json_response(
            client
                .post(format!("{base}/upload/one-shot/{path}?durability=strict"))
                .bearer_auth(&token)
                .body(bytes.clone())
                .send()
                .await
                .unwrap(),
            201,
        )
        .await;
        objects.push((path, bytes, ack["version_id"].as_str().unwrap().to_owned()));
    }
    for lost in &roots {
        drop(stack.metadata.take());
        drop(stack.object.take());
        let detached = lost.with_extension("detached-evidence");
        std::fs::rename(lost, &detached).unwrap();
        stack.start_object().await;
        stack.start_metadata().await;
        for (path, expected, version) in &objects {
            let response = client
                .get(format!("{base}/objects/get/{path}"))
                .bearer_auth(&token)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            assert_eq!(
                response.headers()["etag"]
                    .to_str()
                    .unwrap()
                    .trim_matches('"'),
                version
            );
            assert_eq!(response.bytes().await.unwrap().as_ref(), expected);
        }
        assert!(
            !lost.exists(),
            "normal startup/read silently enrolled a replacement"
        );
        drop(stack.metadata.take());
        drop(stack.object.take());
        std::fs::rename(detached, lost).unwrap();
        stack.start_object().await;
        stack.start_metadata().await;
    }
    // Fault actual immutable shard files. Native reads/repair must authenticate
    // decoded source bytes; a successful HTTP header alone is not a passing GET.
    drop(stack.metadata.take());
    let config = m1::server::ServerConfig::load(&stack.root.join("m1_config.ini")).unwrap();
    let db = MetadataEngine::open(&config.data_dir, Options::default()).unwrap();
    let fragment = m1::fragments::resolve(&db, "Member fault matrix").unwrap();
    let rows = db
        .snapshot()
        .scan(Table::Versions, fragment.id, "", None, 1000)
        .unwrap();
    let keys: Vec<String> = rows
        .into_iter()
        .filter_map(|(_, bytes)| {
            let row: Value = serde_json::from_slice(&bytes).unwrap();
            row["object_key"].as_str().map(str::to_owned)
        })
        .collect();
    drop(db);
    assert_eq!(keys.len(), objects.len());
    stack.start_metadata().await;
    let mut native = direct(&stack).await;
    // Obtain only the fingerprint from a public key identity. No production
    // metadata code opens or scans the member files.
    let signer_bytes: [u8; 32] = std::fs::read(settings["signing_key_file"].as_str().unwrap())
        .unwrap()
        .try_into()
        .unwrap();
    let signer = n1_link::KeySigner::new(signer_bytes);
    let scope: PoolId = serde_json::from_value(settings["fragments"][0]["id"].clone()).unwrap();
    let layout: n1_link::PoolLayout =
        serde_json::from_value(settings["pools"][0]["layout"].clone()).unwrap();
    let mut cases = 0;
    for key in &keys {
        let identity = signer.verify_object(&scope.0, key).unwrap();
        let fp = identity.fingerprint();
        let paths: Vec<_> = (0..2u16)
            .map(|shard| roots[layout.member_slot(fp, shard).unwrap()].join(fp.shard_path(shard)))
            .collect();
        let originals: Vec<_> = paths.iter().map(|p| std::fs::read(p).unwrap()).collect();
        let mut expected = Vec::new();
        native
            .download(key.clone(), 0, None, &mut expected)
            .await
            .unwrap();
        for shard in 0..2 {
            let mut damaged = originals[shard].clone();
            damaged[8] ^= 1;
            std::fs::write(&paths[shard], damaged).unwrap();
            let mut returned = Vec::new();
            native
                .download(key.clone(), 0, None, &mut returned)
                .await
                .unwrap();
            assert_eq!(returned, expected);
            native
                .control(ObjectCommand::ObjectRepair {
                    object_key: key.clone(),
                })
                .await
                .unwrap();
            for (path, bytes) in paths.iter().zip(&originals) {
                assert_eq!(std::fs::read(path).unwrap(), *bytes);
            }
            cases += 1;
        }
        // Both invalid headers exceed 1+1 redundancy. Preserve the damaged
        // bytes as evidence and prove neither GET nor repair claims success.
        for (path, original) in paths.iter().zip(&originals) {
            let mut bytes = original.clone();
            bytes[8] ^= 1;
            std::fs::write(path, bytes).unwrap();
        }
        let mut returned = Vec::new();
        assert!(
            native
                .download(key.clone(), 0, None, &mut returned)
                .await
                .is_err()
        );
        assert!(returned.is_empty());
        assert!(
            native
                .control(ObjectCommand::ObjectRepair {
                    object_key: key.clone()
                })
                .await
                .is_err()
        );
        for (path, original) in paths.iter().zip(&originals) {
            let mut bytes = original.clone();
            bytes[8] ^= 1;
            assert_eq!(
                std::fs::read(path).unwrap(),
                bytes,
                "failed repair rewrote evidence"
            );
            std::fs::write(path, original).unwrap();
        }
        cases += 1;
    }
    drop(native);
    drop(stack.metadata.take());
    drop(stack.object.take());
    stack.start_object().await;
    stack.start_metadata().await;
    for (path, bytes, version) in &objects {
        let response = client
            .get(format!("{base}/objects/get/{path}"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.headers()["etag"]
                .to_str()
                .unwrap()
                .trim_matches('"'),
            version
        );
        assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
    }
    record(
        &stack.root.join("MEMBER-LOSS-PASSED.json"),
        &json!({
            "passed": true,
            "member_unavailability_cases": 2,
            "named_objects_verified_each_time": objects.len(),
            "corruption_and_over_budget_cases": cases,
            "silent_success_beyond_parity": false,
            "final_cold_restart_exact_bytes_and_versions": true,
            "fault_method": "private member root detached, no physical cable or device power interruption"
        }),
    );
}
