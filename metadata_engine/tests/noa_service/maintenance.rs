//! Actual public maintenance against independent object and metadata processes.
use super::*;
use m1::{MetadataEngine, Options, Table};
use n1_link::{PoolId, noa::Command as ObjectCommand};

pub(super) async fn auth(client: &Client, base: &str, name: &str) -> (String, String, String) {
    let fragment = json_response(
        client
            .post(format!("{base}/fragments"))
            .json(&json!({"name":name,"secret":"test-only-master","key":"test-only-fragment"}))
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
    let alias = fragment["fragment_hash"].as_str().unwrap().to_owned();
    let mut tokens = Vec::new();
    for secret in ["test-only-fragment", "test-only-master"] {
        let token=json_response(client.post(format!("{base}/auth/fragment")).json(&json!({"fragment":alias,"secret":secret,"read":true,"write":true,"modify":true,"delete":true})).send().await.unwrap(),200).await;
        tokens.push(token["access_token"].as_str().unwrap().to_owned());
    }
    (alias, tokens.remove(0), tokens.remove(0))
}
async fn wait(client: &Client, base: &str, token: &str, kind: &str) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
    loop {
        let value = json_response(
            client
                .get(format!("{base}/maintenance/{kind}/status"))
                .bearer_auth(token)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        if value["running"] == false {
            return value;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "job timeout: {value}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
fn policy(stack: &Stack, enabled: bool, ttl: u64, interval: u64, pause: u64) {
    let path = stack.root.join("m1_config.ini");
    let mut ini = ini::Ini::load_from_file(&path).unwrap();
    ini.with_section(Some("garbage_collector"))
        .set("enabled", enabled.to_string())
        .set("trash_retention_seconds", ttl.to_string())
        .set("cleanup_interval_seconds", interval.to_string())
        .set("batch_size", "2")
        .set("object_concurrency", "2")
        .set("pause_millis", pause.to_string())
        .set("bytes_per_second", "67108864");
    ini.write_to_file(path).unwrap();
}
pub(super) async fn direct(stack: &Stack) -> n1_native_client::N1NativeClient {
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
pub(super) fn db(stack: &Stack) -> MetadataEngine {
    MetadataEngine::open(
        stack.root.join("metadata-control/metadata"),
        Options::default(),
    )
    .unwrap()
}
pub(super) fn links(database: &MetadataEngine) -> Vec<String> {
    let mut after = None;
    let mut keys = Vec::new();
    // The test keeps its expected fragment IDs in the independent request oracle.
    let fragments = database
        .snapshot()
        .scan(Table::Reclamation, PoolId([0; 16]), "fragment/", None, 1000)
        .unwrap();
    for (_, bytes) in fragments {
        let fragment: Value = serde_json::from_slice(&bytes).unwrap();
        let id: PoolId = serde_json::from_value(fragment["id"].clone()).unwrap();
        loop {
            let rows = database
                .snapshot()
                .scan(Table::Uploads, id, "", after.as_ref(), 1000)
                .unwrap();
            if rows.is_empty() {
                break;
            }
            after = rows.last().map(|r| r.0.clone());
            for (_, bytes) in rows {
                let pending: Value = serde_json::from_slice(&bytes).unwrap();
                keys.push(pending["upload"]["object_key"].as_str().unwrap().into());
            }
        }
        after = None;
    }
    keys
}
pub(super) async fn put(
    client: &Client,
    base: &str,
    token: &str,
    path: &str,
    bytes: &[u8],
) -> String {
    let ack = json_response(
        client
            .post(format!("{base}/upload/one-shot/{path}?durability=strict"))
            .bearer_auth(token)
            .body(bytes.to_vec())
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
    ack["version_id"].as_str().unwrap().into()
}
async fn start(client: &Client, base: &str, token: &str, kind: &str, query: &str) -> Value {
    json_response(
        client
            .post(format!("{base}/maintenance/{kind}/start?{query}"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "private two-HDD TTL, durable purge intent, partial removal/restart, permissions and scheduling"]
async fn indexed_trash_gc_enforces_ttl_retries_after_outage_and_automatic_schedule() {
    let mut stack = Stack::new().await;
    drop(stack.metadata.take());
    policy(&stack, false, 3, 2, 5);
    stack.start_metadata().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (alias, token, operator) = auth(&client, &base, "GC witness").await;
    let (other, other_token, _) = auth(&client, &base, "Other fragment").await;
    let one = vec![17; 65537];
    let two = vec![29; 131195];
    put(&client, &base, &token, "remove.bin", &one).await;
    put(&client, &base, &token, "remove.bin", &two).await;
    put(&client, &base, &token, "recover.bin", &one).await;
    put(&client, &base, &other_token, "live.bin", &two).await;
    drop(stack.metadata.take());
    let before = links(&db(&stack));
    assert_eq!(before.len(), 4);
    stack.start_metadata().await;
    for path in ["remove.bin", "recover.bin"] {
        json_response(
            client
                .delete(format!("{base}/objects/{path}"))
                .bearer_auth(&token)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
    }
    start(&client, &base, &token, "gc", "").await;
    let early = wait(&client, &base, &token, "gc").await;
    assert_eq!(early["processed"], 0, "{early}");
    assert_eq!(
        client
            .post(format!("{base}/maintenance/gc/start?all_fragments=true"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        client
            .post(format!("{base}/maintenance/fragments/{other}/gc/start"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        client
            .post(format!(
                "{base}/maintenance/gc/start?fragment={alias}&batch=0"
            ))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    json_response(
        client
            .post(format!("{base}/objects/recover/recover.bin"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    start(&client, &base, &token, "gc", "dry_run=true").await;
    let dry = wait(&client, &base, &token, "gc").await;
    assert_eq!(dry["phase"], "completed");
    drop(stack.object.take());
    start(&client, &base, &token, "gc", "").await;
    let failed = wait(&client, &base, &token, "gc").await;
    assert_eq!(failed["phase"], "incomplete", "{failed}");
    assert!(failed["failures"].as_u64().unwrap() > 0);
    let conflict = json_response(
        client
            .post(format!("{base}/objects/recover/remove.bin"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(conflict["affected"], 0);
    assert_eq!(conflict["conflicted_paths"], json!(["remove.bin"]));
    drop(stack.metadata.take());
    let database = db(&stack);
    assert_eq!(links(&database).len(), 4);
    // A failed physical purge must retain the durable last-owner intent and
    // both version keys. Model an object process crash after one member's
    // unlink, with a still-readable survivor in this 1+1 pool.
    let fragment = m1::fragments::resolve(&database, &alias).unwrap();
    let purging = database
        .snapshot()
        .scan(
            Table::Reclamation,
            PoolId([0; 16]),
            "link-purging/",
            None,
            10,
        )
        .unwrap();
    assert_eq!(purging.len(), 1);
    let removed_keys = database
        .snapshot()
        .scan(Table::Uploads, fragment.id, "", None, 10)
        .unwrap()
        .into_iter()
        .filter_map(|(_, bytes)| {
            let pending: Value = serde_json::from_slice(&bytes).unwrap();
            (pending["handle"]["object_key"] == "remove.bin")
                .then(|| pending["upload"]["object_key"].as_str().unwrap().to_owned())
        })
        .collect::<Vec<_>>();
    assert_eq!(removed_keys.len(), 2);
    drop(database);
    let settings: Value =
        serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
    let layout: n1_link::PoolLayout =
        serde_json::from_value(settings["pools"][0]["layout"].clone()).unwrap();
    let roots: Vec<PathBuf> =
        serde_json::from_value(settings["pools"][0]["roots"].clone()).unwrap();
    for key in &removed_keys {
        let fp = n1_link::ObjectKey::inspect(key).unwrap().fingerprint();
        let path = roots[layout.member_slot(fp, 0).unwrap()].join(fp.shard_path(0));
        std::fs::remove_file(&path).unwrap();
        std::fs::File::open(path.parent().unwrap())
            .unwrap()
            .sync_all()
            .unwrap();
    }
    stack.start_object().await;
    stack.start_metadata().await;
    start(&client, &base, &token, "gc", "").await;
    let completed = wait(&client, &base, &token, "gc").await;
    assert_eq!(completed["phase"], "completed", "{completed}");
    for key in &removed_keys {
        let fp = n1_link::ObjectKey::inspect(key).unwrap().fingerprint();
        for root in &roots {
            assert!(!root.join(format!(".uploads/{fp}")).exists());
            for shard in 0..layout.width() {
                assert!(!root.join(fp.shard_path(shard as u16)).exists());
            }
        }
    }
    let mut native = direct(&stack).await;
    let mut retired = 0;
    for key in &before {
        let result = native
            .control(ObjectCommand::ObjectStat {
                object_key: key.clone(),
            })
            .await;
        if matches!(result,Err(n1_native_client::NativeClientError::Native{code,..})if code=="404")
        {
            retired += 1;
        }
    }
    assert_eq!(retired, 2);
    assert_eq!(
        client
            .get(format!("{base}/objects/get/recover.bin"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        one
    );
    assert_eq!(
        client
            .get(format!("{base}/objects/get/live.bin"))
            .bearer_auth(&other_token)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        two
    );
    drop(stack.metadata.take());
    let database = db(&stack);
    assert_eq!(links(&database).len(), 2);
    drop(database);
    policy(&stack, true, 1, 1, 5);
    stack.start_metadata().await;
    json_response(
        client
            .delete(format!("{base}/objects/recover.bin"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let state = json_response(
            client
                .get(format!("{base}/maintenance/gc/status"))
                .bearer_auth(&operator)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        if state["phase"] == "completed" && state["processed"].as_u64().unwrap_or(0) > 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "automatic GC did not reclaim: {state}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        client
            .get(format!("{base}/objects/get/live.bin"))
            .bearer_auth(&other_token)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        two
    );
    std::fs::write(stack.root.join("GC-PASSED.json"),serde_json::to_vec_pretty(&json!({"passed":true,"failed_attempt":failed,"completed":completed,"retired_keys":retired})).unwrap()).unwrap();
}

async fn add_destination(stack: &mut Stack) -> (String, String) {
    drop(stack.metadata.take());
    drop(stack.object.take());
    let mut original: Value =
        serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
    let source = original["pools"][0]["layout"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let parents = if let Some(paths) = std::env::var_os("N1_OBJECT_TEST_FOUR_ROOTS") {
        std::env::split_paths(&paths).take(2).collect::<Vec<_>>()
    } else {
        ["N1_OBJECT_TEST_FIRST_ROOT", "N1_OBJECT_TEST_SECOND_ROOT"]
            .map(|name| {
                PathBuf::from(
                    std::env::var_os(name).expect("explicit migration member parent required"),
                )
            })
            .to_vec()
    };
    assert_eq!(parents.len(), 2);
    let first = parents[0].join(format!("migration-{}", ulid::Ulid::new()));
    let second = parents[1].join(format!("migration-{}", ulid::Ulid::new()));
    std::fs::create_dir(&first).unwrap();
    std::fs::create_dir(&second).unwrap();
    let config = stack.root.join("destination.json");
    let output = Command::new(&stack.object_binary)
        .args(["init", "--config"])
        .arg(&config)
        .arg("--member")
        .arg(&first)
        .arg("--member")
        .arg(&second)
        .args([
            "--data-shards",
            "2",
            "--parity-shards",
            "0",
            "--block-bytes",
            "4096",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let destination: Value = serde_json::from_slice(&std::fs::read(config).unwrap()).unwrap();
    let mut pool = destination["pools"][0].clone();
    pool["sealed"] = json!(false);
    let target = pool["layout"]["id"].as_str().unwrap().to_owned();
    original["pools"].as_array_mut().unwrap().push(pool);
    std::fs::write(
        &stack.object_config,
        serde_json::to_vec_pretty(&original).unwrap(),
    )
    .unwrap();
    stack.start_object().await;
    stack.start_metadata().await;
    (source, target)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "private two-HDD 1+1 to 2+0 migration, exact history, cancellation and actual metadata process SIGKILL"]
async fn pool_migration_changes_ec_preserves_versions_and_resumes_after_process_kill() {
    let mut stack = Stack::new().await;
    drop(stack.metadata.take());
    policy(&stack, false, 1, 3600, 10);
    stack.start_metadata().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token, operator) = auth(&client, &base, "Migration witness").await;
    let mut oracle = Vec::new();
    for (path, bytes) in [
        ("empty", vec![]),
        ("file", vec![7; 262195]),
        ("file", vec![13; 131199]),
        ("large", vec![31; 17 * 1024 * 1024 + 19]),
    ] {
        let version = put(&client, &base, &token, path, &bytes).await;
        oracle.push((path.to_owned(), version, bytes));
    }
    drop(stack.metadata.take());
    let before = links(&db(&stack));
    stack.start_metadata().await;
    let (source, target) = add_destination(&mut stack).await;
    assert_eq!(
        client
            .post(format!(
                "{base}/maintenance/redistribution/start?source_pool={source}&target_pool={source}"
            ))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    let query = format!("source_pool={source}&target_pool={target}&batch=2&object_concurrency=2");
    start(
        &client,
        &base,
        &token,
        "redistribution",
        &format!("{query}&dry_run=true"),
    )
    .await;
    assert_eq!(
        wait(&client, &base, &token, "redistribution").await["phase"],
        "completed"
    );
    start(&client, &base, &token, "redistribution", &query).await;
    assert_eq!(
        client
            .post(format!("{base}/maintenance/redistribution/start?{query}"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    let settings: Value =
        serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
    let roots = settings["pools"][1]["roots"].as_array().unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let staged = roots.iter().any(|root| {
            std::fs::read_dir(Path::new(root.as_str().unwrap()).join(".uploads"))
                .ok()
                .is_some_and(|rows| {
                    rows.filter_map(Result::ok).any(|row| {
                        row.path()
                            .join("shard.tmp")
                            .metadata()
                            .is_ok_and(|m| m.len() > 4096)
                    })
                })
        });
        if staged {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "migration never reached a written destination staging file"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    drop(stack.metadata.take()); // actual SIGKILL with destination bytes in flight
    stack.start_metadata().await;
    let done = wait(&client, &base, &operator, "redistribution").await;
    assert_eq!(done["phase"], "completed", "{done}");
    for (path, version, bytes) in &oracle {
        let response = client
            .get(format!("{base}/objects/get/{path}?version_id={version}"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
    }
    let mut native = direct(&stack).await;
    for key in &before {
        assert!(
            matches!(native.control(ObjectCommand::ObjectStat{object_key:key.clone()}).await,Err(n1_native_client::NativeClientError::Native{code,..})if code=="404")
        );
    }
    drop(stack.metadata.take());
    let database = db(&stack);
    let after = links(&database);
    assert_eq!(after.len(), 4);
    for key in &after {
        assert_eq!(
            n1_link::ObjectKey::inspect(key).unwrap().pool.to_string(),
            target
        );
    }
    drop(database);
    stack.start_metadata().await;
    // An explicit stop retires only an unfinished destination. Every named
    // version remains readable whichever pool won its per-version boundary.
    let reverse = format!("source_pool={target}&target_pool={source}&batch=2&object_concurrency=1");
    start(&client, &base, &token, "redistribution", &reverse).await;
    json_response(
        client
            .post(format!("{base}/maintenance/redistribution/stop"))
            .bearer_auth(&operator)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(
        wait(&client, &base, &operator, "redistribution").await["phase"],
        "stopped"
    );
    for (path, version, bytes) in &oracle {
        let response = client
            .get(format!("{base}/objects/get/{path}?version_id={version}"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
    }
    std::fs::write(
        stack.root.join("MIGRATION-PASSED.json"),
        serde_json::to_vec_pretty(&json!({
            "passed": true,
            "source": source,
            "destination": target,
            "versions": oracle.len(),
            "job": done,
            "real_sigkill": true
        }))
        .unwrap(),
    )
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "private two-HDD scoped check-only and explicit healing; no automatic read repair"]
async fn integrity_check_reports_damage_and_scoped_healing_preserves_other_fragments() {
    let mut stack = Stack::new().await;
    drop(stack.metadata.take());
    drop(stack.object.take());
    policy(&stack, false, 1, 3600, 1);
    let mut config: Value =
        serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
    config["read_repair"] = json!(false);
    std::fs::write(
        &stack.object_config,
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();
    stack.start_object().await;
    stack.start_metadata().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token, _) = auth(&client, &base, "Heal selected").await;
    let (_, other, _) = auth(&client, &base, "Leave unchanged").await;
    let bytes = vec![41; 131199];
    let selected = put(&client, &base, &token, "heal.bin", &bytes).await;
    put(&client, &base, &other, "other.bin", &bytes).await;
    drop(stack.metadata.take());
    drop(stack.object.take());
    let database = db(&stack);
    let keys = links(&database);
    drop(database);
    assert_eq!(keys.len(), 2);
    let layout: n1_link::PoolLayout =
        serde_json::from_value(config["pools"][0]["layout"].clone()).unwrap();
    let first = config["pools"][0]["roots"][0].as_str().unwrap();
    let mut removed = Vec::new();
    for key in keys {
        let fingerprint = n1_link::ObjectKey::inspect(&key).unwrap().fingerprint();
        let shard = layout.shard_at_slot(fingerprint, 0).unwrap();
        let path = Path::new(first).join(fingerprint.shard_path(shard));
        let backup = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        removed.push((path, backup));
    }
    stack.start_object().await;
    stack.start_metadata().await;
    start(&client, &base, &token, "repair", "dry_run=true").await;
    let checked = wait(&client, &base, &token, "repair").await;
    assert_eq!(checked["phase"], "incomplete", "{checked}");
    assert!(removed.iter().all(|(path, _)| !path.exists()));
    start(&client, &base, &token, "repair", "").await;
    let healed = wait(&client, &base, &token, "repair").await;
    assert_eq!(healed["phase"], "completed", "{healed}");
    assert_eq!(removed.iter().filter(|(path, _)| path.exists()).count(), 1);
    for (path, expected) in &removed {
        if path.exists() {
            assert_eq!(std::fs::read(path).unwrap(), *expected);
        }
    }
    let response = client
        .get(format!("{base}/objects/get/heal.bin?version_id={selected}"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
    assert_eq!(removed.iter().filter(|(path, _)| path.exists()).count(), 1);
    std::fs::write(stack.root.join("HEALING-PASSED.json"),serde_json::to_vec_pretty(&json!({"passed":true,"check_only":checked,"healing":healed,"other_fragment_shard_unchanged":true})).unwrap()).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "private two-HDD failed migration, cancellation and source preservation witness"]
async fn failed_migration_retains_source_reference_and_cancel_only_retires_destination() {
    let mut stack = Stack::new().await;
    drop(stack.metadata.take());
    policy(&stack, false, 3600, 3600, 1);
    stack.start_metadata().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token, operator) = auth(&client, &base, "Failed migration").await;
    let bytes = vec![79; 131199];
    let version = put(&client, &base, &token, "preserve.bin", &bytes).await;
    drop(stack.metadata.take());
    let source_key = links(&db(&stack)).remove(0);
    stack.start_metadata().await;
    let (source, target) = add_destination(&mut stack).await;
    drop(stack.metadata.take());
    drop(stack.object.take());
    let settings: Value =
        serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
    let layout: n1_link::PoolLayout =
        serde_json::from_value(settings["pools"][0]["layout"].clone()).unwrap();
    let fingerprint = n1_link::ObjectKey::inspect(&source_key)
        .unwrap()
        .fingerprint();
    let mut evidence = Vec::new();
    for (slot, root) in settings["pools"][0]["roots"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        let path = Path::new(root.as_str().unwrap())
            .join(fingerprint.shard_path(layout.shard_at_slot(fingerprint, slot).unwrap()));
        let bytes = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        evidence.push((path, bytes));
    }
    stack.start_object().await;
    stack.start_metadata().await;
    let query = format!("source_pool={source}&target_pool={target}");
    let admitted = start(&client, &base, &token, "redistribution", &query).await;
    let failed = wait(&client, &base, &token, "redistribution").await;
    assert_eq!(failed["phase"], "incomplete", "{failed}");
    drop(stack.metadata.take());
    let database = db(&stack);
    assert_eq!(links(&database), vec![source_key.clone()]);
    let work = database
        .snapshot()
        .scan(
            Table::Reclamation,
            PoolId([0; 16]),
            &format!("move/{}/", admitted["id"].as_str().unwrap()),
            None,
            10,
        )
        .unwrap();
    assert_eq!(work.len(), 1);
    let intent: Value = serde_json::from_slice(&work[0].1).unwrap();
    assert_eq!(intent["switched"], false);
    let destination_key = intent["upload"]["object_key"].as_str().unwrap().to_owned();
    drop(database);
    stack.start_metadata().await;
    json_response(
        client
            .post(format!("{base}/maintenance/redistribution/stop"))
            .bearer_auth(&operator)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(
        wait(&client, &base, &operator, "redistribution").await["phase"],
        "stopped"
    );
    let mut native = direct(&stack).await;
    assert!(
        matches!(native.control(ObjectCommand::ObjectStat{object_key:destination_key}).await,Err(n1_native_client::NativeClientError::Native{code,..})if code=="404")
    );
    drop(native);
    drop(stack.metadata.take());
    drop(stack.object.take());
    for (path, bytes) in evidence {
        std::fs::write(path, bytes).unwrap();
    }
    assert_eq!(links(&db(&stack)), vec![source_key]);
    stack.start_object().await;
    stack.start_metadata().await;
    let response = client
        .get(format!(
            "{base}/objects/get/preserve.bin?version_id={version}"
        ))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
    std::fs::write(stack.root.join("FAILED-MIGRATION-RECOVERY-PASSED.json"),serde_json::to_vec_pretty(&json!({"passed":true,"failed_job":failed,"source_reference_preserved":true,"destination_retired":true})).unwrap()).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "actual original Console browser -> metadata -> native object GC and migration on private HDD fixtures"]
async fn real_console_runs_pool_migration_and_metadata_trash_gc() {
    let mut stack = Stack::new().await;
    drop(stack.metadata.take());
    policy(&stack, false, 1, 3600, 1);
    stack.start_metadata().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (alias, _, operator) = auth(&client, &base, "Console maintenance witness").await;
    let bytes = vec![23; 131199];
    let version = put(&client, &base, &operator, "console.bin", &bytes).await;
    let (source, target) = add_destination(&mut stack).await;
    let access = stack.root.join("console-maintenance-access.json");
    let console_port = port();
    let settings = json!({
        "metadata": stack.url,
        "fragment": alias,
        "token": operator,
        "source": source,
        "target": target,
        "console_port": console_port,
        "output": stack.root
    });
    std::fs::write(&access, serde_json::to_vec_pretty(&settings).unwrap()).unwrap();
    std::fs::set_permissions(&access, std::fs::Permissions::from_mode(0o600)).unwrap();
    for phase in ["migration", "gc-outage", "gc"] {
        if phase == "gc-outage" {
            json_response(
                client
                    .delete(format!("{base}/objects/console.bin"))
                    .bearer_auth(&operator)
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await;
            tokio::time::sleep(Duration::from_secs(2)).await;
            drop(stack.object.take());
        } else if phase == "gc" {
            stack.start_object().await;
        }
        let executable =
            std::env::var_os("N1_BROWSER_NODE").expect("explicit browser Node required");
        let script =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/noa_service/maintenance_console.mjs");
        let file = std::fs::File::create(stack.root.join(format!("console-{phase}.log"))).unwrap();
        let mut child = Command::new(executable)
            .arg(script)
            .arg(&access)
            .arg(phase)
            .stdout(Stdio::from(file.try_clone().unwrap()))
            .stderr(Stdio::from(file))
            .spawn()
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(
                    status.success(),
                    "{}",
                    std::fs::read_to_string(stack.root.join(format!("console-{phase}.log")))
                        .unwrap()
                );
                break;
            }
            if tokio::time::Instant::now() > deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("Console maintenance timed out");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let response = client
            .get(format!(
                "{base}/objects/get/console.bin?version_id={version}"
            ))
            .bearer_auth(&operator)
            .send()
            .await
            .unwrap();
        if phase == "migration" {
            assert_eq!(response.status(), 200);
            assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
        } else {
            assert_eq!(response.status(), 404);
        }
    }
    std::fs::write(
        stack.root.join("CONSOLE-MAINTENANCE-PASSED.json"),
        serde_json::to_vec_pretty(&json!({
            "passed": true,
            "real_console": true,
            "real_services": true,
            "metadata_gc": true,
            "gc_outage_and_retry": true,
            "cross_ec_migration": true
        }))
        .unwrap(),
    )
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "actual short-lived durable upload expiry, backend outage, restart and indexed metadata cleanup"]
async fn expired_partial_upload_cleanup_survives_backend_outage_and_metadata_restart() {
    let mut stack = Stack::new().await;
    drop(stack.metadata.take());
    policy(&stack, false, 3600, 3600, 1);
    stack.start_metadata().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (alias, token, _) = auth(&client, &base, "Expired upload witness").await;
    drop(stack.metadata.take());
    let database = db(&stack);
    let fragment = m1::fragments::resolve(&database, &alias).unwrap().id;
    let executor = m1::MetadataExecutor::new(database.clone(), 4).unwrap();
    let credentials: Value = serde_json::from_slice(
        &std::fs::read(stack.object_config.with_extension("credentials.json")).unwrap(),
    )
    .unwrap();
    let objects = m1::object_store::NativeObjectStore::new(m1::object_store::ObjectStoreConfig {
        native_url: credentials["native_url"].as_str().unwrap().into(),
        fragment: credentials["fragment"].as_str().unwrap().into(),
        secret: credentials["secret"].as_str().unwrap().into(),
        max_connections: 4,
        tls: None,
    })
    .unwrap();
    let service = m1::service::MetadataService::new(executor, objects);
    let settings: Value =
        serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
    let part_bytes = settings["pools"][0]["layout"]["data_shards"]
        .as_u64()
        .unwrap()
        * 4096;
    let upload = service
        .begin_upload(m1::service::UploadRequest {
            fragment,
            path: "abandoned.bin".into(),
            content_type: "application/octet-stream".into(),
            pool: None,
            total_bytes: part_bytes * 2,
            part_bytes,
            durable: true,
            ttl_seconds: 3,
        })
        .await
        .unwrap();
    service
        .upload_part(
            fragment,
            &upload.version_id,
            0,
            part_bytes,
            &mut std::io::Cursor::new(vec![17; part_bytes as usize]),
        )
        .await
        .unwrap();
    let key = links(&database).remove(0);
    service.shutdown().await;
    drop(service);
    drop(database);
    tokio::time::sleep(Duration::from_secs(4)).await;
    stack.start_metadata().await;
    drop(stack.object.take());
    start(&client, &base, &token, "gc", "").await;
    let failed = wait(&client, &base, &token, "gc").await;
    assert_eq!(failed["phase"], "incomplete", "{failed}");
    drop(stack.metadata.take());
    stack.start_object().await;
    stack.start_metadata().await;
    start(&client, &base, &token, "gc", "").await;
    let done = wait(&client, &base, &token, "gc").await;
    assert_eq!(done["phase"], "completed", "{done}");
    let mut native = direct(&stack).await;
    assert!(
        matches!(native.control(ObjectCommand::ObjectStat{object_key:key}).await,Err(n1_native_client::NativeClientError::Native{code,..})if code=="404")
    );
    drop(native);
    drop(stack.metadata.take());
    let database = db(&stack);
    assert!(links(&database).is_empty());
    assert!(
        database
            .snapshot()
            .scan(Table::Entries, fragment, "", None, 10)
            .unwrap()
            .is_empty()
    );
    drop(database);
    stack.start_metadata().await;
    std::fs::write(
        stack.root.join("EXPIRED-UPLOAD-PASSED.json"),
        serde_json::to_vec_pretty(
            &json!({"passed":true,"real_ticket_expiry":true,"failed_job":failed,"completed":done}),
        )
        .unwrap(),
    )
    .unwrap();
}
