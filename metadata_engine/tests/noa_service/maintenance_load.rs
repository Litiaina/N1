//! Sustained public M1 -> N1 traffic alongside explicit, bounded maintenance.
//! Reuses the real-process fixture; never changes either engine implementation.
use super::*;
use sha2::{Digest, Sha256};
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};
#[path = "maintenance_load/observe.rs"]
mod observe;
#[path = "maintenance_load/workload.rs"]
mod workload;
use observe::Meter;

#[derive(Clone, serde::Serialize)]
struct Expected {
    path: String,
    version: String,
    bytes: u64,
    seed: u64,
    sha256: String,
    trashed: bool,
}

#[derive(Clone)]
struct Context {
    client: Client,
    base: String,
    token: String,
    operator: String,
    oracle: Arc<Mutex<Vec<Expected>>>,
}

impl Context {
    fn save(&self, expected: Expected) -> usize {
        let mut oracle = self.oracle.lock().unwrap();
        assert!(oracle.len() < 20_000, "bounded object count exceeded");
        assert!(
            oracle.iter().map(|row| row.bytes).sum::<u64>() + expected.bytes < 20 << 30,
            "bounded fixture byte capacity exceeded"
        );
        let index = oracle.len();
        oracle.push(expected);
        index
    }

    async fn gc(&self) -> Value {
        json_response(
            self.client
                .post(format!("{}/maintenance/gc/start", self.base))
                .bearer_auth(&self.token)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        let deadline = Instant::now() + Duration::from_secs(180);
        loop {
            let status = json_response(
                self.client
                    .get(format!("{}/maintenance/gc/status", self.base))
                    .bearer_auth(&self.token)
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await;
            if status["running"] == false {
                assert_eq!(status["failures"], 0, "{status}");
                assert!(
                    matches!(status["phase"].as_str(), Some("completed" | "incomplete")),
                    "{status}"
                );
                return status;
            }
            assert!(Instant::now() < deadline, "GC timed out: {status}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn backup(&self) -> String {
        let route = format!("{}/maintenance/metadata/backups", self.base);
        let job = json_response(
            self.client
                .post(&route)
                .bearer_auth(&self.operator)
                .send()
                .await
                .unwrap(),
            202,
        )
        .await;
        let id = job["id"].as_str().unwrap().to_owned();
        let deadline = Instant::now() + Duration::from_secs(180);
        loop {
            let list = json_response(
                self.client
                    .get(&route)
                    .bearer_auth(&self.operator)
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await;
            assert_eq!(list["job"]["id"], id);
            match list["job"]["phase"].as_str().unwrap() {
                "completed" => return id,
                "running" => (),
                _ => panic!("backup failed: {list}"),
            }
            assert!(Instant::now() < deadline, "backup timed out");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

async fn maintenance_tasks(
    context: Context,
    meter: Arc<Meter>,
    stop: Instant,
    tasks: &mut tokio::task::JoinSet<()>,
    snapshots: Arc<Mutex<Vec<String>>>,
) {
    let (monitor, monitor_meter) = (context.clone(), meter.clone());
    tasks.spawn(async move {
        let mut index = 0;
        while Instant::now() < stop {
            let route = [
                "maintenance/health?refresh=true",
                "maintenance/pools/health?deep_scan=false",
                "capacity/fragment",
            ][index % 3];
            monitor_meter
                .call(5, "monitoring", 0, async {
                    let health = json_response(
                        monitor
                            .client
                            .get(format!("{}/{route}", monitor.base))
                            .bearer_auth(&monitor.token)
                            .send()
                            .await
                            .unwrap(),
                        200,
                    )
                    .await;
                    assert_ne!(health["status"], "critical", "{health}");
                    if route.starts_with("maintenance/health") {
                        assert_eq!(health["monitoring"]["automatic_content_scan"], false);
                        // Cached observations may become stale under load.
                        // Preserve that adverse telemetry rather than treating
                        // its age as evidence of damaged data or hiding it.
                        let meta = &health["components"]["metadata"];
                        assert!(meta["error"].is_null(), "{meta}");
                        assert!(meta["storage"]["database_error"].is_null(), "{meta}");
                        for disk in meta["storage"]["disks"].as_array().into_iter().flatten() {
                            assert_eq!(disk["status"], "online", "{disk}");
                        }
                    }
                    monitor_meter.observation(health);
                })
                .await;
            index += 1;
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    });
    let (collector, gc_meter) = (context.clone(), meter.clone());
    tasks.spawn(async move {
        while Instant::now() < stop {
            gc_meter.call(6, "gc", 0, collector.gc()).await;
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });
    tasks.spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let id = meter.call(7, "backup", 0, context.backup()).await;
        snapshots.lock().unwrap().push(id);
    });
}

fn marker_only(root: &Path) -> usize {
    let mut files = 0;
    for entry in std::fs::read_dir(root).unwrap() {
        let entry = entry.unwrap();
        let kind = entry.file_type().unwrap();
        assert!(!kind.is_symlink());
        if kind.is_dir() {
            files += marker_only(&entry.path());
        } else {
            assert!(kind.is_file());
            assert_eq!(
                entry.file_name(),
                ".n1-arc-pool.json",
                "residue: {}",
                entry.path().display()
            );
            files += 1;
        }
    }
    files
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "four sustained on/off phases, full restart readback, live snapshots and exact GC census"]
async fn mixed_traffic_with_gc_monitoring_and_backups_preserves_every_ack() {
    let seconds = std::env::var("M1_MAINTENANCE_SECONDS")
        .map(|s| s.parse::<u64>().unwrap())
        .unwrap_or(60);
    assert!((3..=600).contains(&seconds));
    let mut stack = Stack::with_block_bytes(262144).await;
    drop(stack.metadata.take());
    let ini_path = stack.root.join("m1_config.ini");
    let mut ini = ini::Ini::load_from_file(&ini_path).unwrap();
    ini.with_section(Some("garbage_collector"))
        .set("enabled", "false")
        .set("trash_retention_seconds", "0")
        .set("batch_size", "32")
        .set("object_concurrency", "1")
        .set("pause_millis", "25");
    ini.write_to_file(&ini_path).unwrap();
    let original = m1::server::ServerConfig::load(&ini_path).unwrap().data_dir;
    stack.start_metadata().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token, operator) = maintenance::auth(&client, &base, "Concurrent maintenance").await;
    let context = Context {
        client,
        base,
        token,
        operator,
        oracle: Arc::new(Mutex::new(Vec::new())),
    };
    let mut library = Vec::new();
    for index in 0..2 {
        let expected = workload::upload(
            &context,
            format!("library/{index}.bin"),
            8 << 20,
            index as u64,
            "strict",
        )
        .await;
        context.save(expected.clone());
        library.push(expected);
    }
    let pids = [
        stack.metadata.as_ref().unwrap().0.id(),
        stack.object.as_ref().unwrap().0.id(),
    ];
    let snapshots = Arc::new(Mutex::new(Vec::new()));
    let mut phases = Vec::new();
    for (phase, maintenance) in [false, true, true, false].into_iter().enumerate() {
        let name = format!("phase-{phase}");
        let meter = Meter::new();
        let start = Instant::now();
        let stop = start + Duration::from_secs(seconds);
        let before = pids.map(preparation_bench::resources);
        let mut tasks = tokio::task::JoinSet::new();
        workload::foreground(
            context.clone(),
            meter.clone(),
            stop,
            phase,
            library.clone(),
            &mut tasks,
        )
        .await;
        if maintenance {
            maintenance_tasks(
                context.clone(),
                meter.clone(),
                stop,
                &mut tasks,
                snapshots.clone(),
            )
            .await;
        }
        let mut resources = Vec::new();
        let mut failures = Vec::new();
        loop {
            tokio::select! {
                finished = tasks.join_next() => match finished {
                    None => break,
                    Some(Ok(())) => (),
                    Some(Err(error)) => failures.push(error.to_string()),
                },
                _ = tokio::time::sleep(Duration::from_millis(250)) => {
                    resources.push(json!({"elapsed_ms":start.elapsed().as_secs_f64()*1000.,
                        "processes":pids.map(preparation_bench::resources)}));
                }
            }
        }
        let mut result = meter.finish(&stack.root, &name, seconds);
        result["offered_seconds"] = json!(seconds);
        result["wall_seconds"] = json!(start.elapsed().as_secs_f64());
        result["maintenance"] = json!(maintenance);
        result["before"] = json!(before);
        result["after"] = json!(pids.map(preparation_bench::resources));
        result["worker_failures"] = json!(failures);
        reliability::record(
            &stack.root.join(format!("{name}-resources.json")),
            &resources,
        );
        reliability::record(&stack.root.join(format!("{name}-summary.json")), &result);
        eprintln!(
            "maintenance phase {phase}: maintenance={maintenance}, seconds={}, requests={}, errors={}",
            result["wall_seconds"], result["samples"], result["errors"]
        );
        assert!(failures.is_empty(), "{failures:?}");
        assert_eq!(result["errors"], 0);
        if maintenance {
            assert!(result["foreground_maintenance_overlaps"].as_u64().unwrap() > 0);
        }
        phases.push(result);
    }
    let oracle = context.oracle.lock().unwrap().clone();
    reliability::record(&stack.root.join("acknowledged.json"), &oracle);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let health = json_response(
            context
                .client
                .get(format!("{}/maintenance/health", context.base))
                .bearer_auth(&context.token)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        if health["components"]["metadata"]["status"] == "healthy"
            && health["components"]["object"]["status"] == "healthy"
        {
            reliability::record(&stack.root.join("idle-health.json"), &health);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "idle monitoring did not recover: {health}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    // Process kills preserve the independent oracle. No payload is rewritten
    // before the complete published-version/absence verification below.
    drop(stack.metadata.take());
    drop(stack.object.take());
    let db = maintenance::db(&stack);
    db.verify().unwrap();
    drop(db);
    stack.start_object().await;
    stack.start_metadata().await;
    for expected in &oracle {
        workload::verify(&context, expected, false).await;
    }
    // Concurrent snapshots are MVCC metadata captures, not coordinated blob
    // backups. Verify their trees and the never-trashed library through NOA.
    let snapshots = snapshots.lock().unwrap().clone();
    assert_eq!(snapshots.len(), 2);
    for id in &snapshots {
        drop(stack.metadata.take());
        let target = stack.root.join(format!("restored-{id}"));
        let archive = stack
            .root
            .join("metadata-backups")
            .join(format!("{id}.n1meta"));
        assert!(m1::MetadataEngine::restore_backup(&archive, &target).unwrap() > 0);
        let db = m1::MetadataEngine::open(&target, m1::Options::default()).unwrap();
        db.verify().unwrap();
        drop(db);
        ini.with_section(Some("metadata_engine"))
            .set("data_dir", target.display().to_string());
        ini.write_to_file(&ini_path).unwrap();
        stack.start_metadata().await;
        for expected in &library {
            workload::verify(&context, expected, false).await;
        }
    }
    drop(stack.metadata.take());
    ini.with_section(Some("metadata_engine"))
        .set("data_dir", original.display().to_string());
    ini.write_to_file(&ini_path).unwrap();
    stack.start_metadata().await;
    for expected in oracle.iter().filter(|row| !row.trashed) {
        json_response(
            context
                .client
                .delete(format!("{}/objects/{}", context.base, expected.path))
                .bearer_auth(&context.token)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
    }
    let gc = context.gc().await;
    assert_eq!(gc["phase"], "completed", "{gc}");
    assert_eq!(gc["deferred"], 0, "{gc}");
    drop(stack.metadata.take());
    drop(stack.object.take());
    let db = maintenance::db(&stack);
    db.verify().unwrap();
    assert!(maintenance::links(&db).is_empty());
    drop(db);
    let config: Value =
        serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
    let roots = config["pools"][0]["roots"].as_array().unwrap();
    for root in roots {
        assert_eq!(marker_only(Path::new(root.as_str().unwrap())), 1);
    }
    reliability::record(
        &stack.root.join("MAINTENANCE-LOAD-PASSED.json"),
        &json!({
            "passed":true,"phases":phases,"identities":oracle.len(),"snapshots_verified":snapshots.len(),
            "post_sigkill_all_payloads_versions_and_absences_verified":true,
            "final_gc":gc,"member_markers_only":roots.len(),
            "scope":"loopback public HTTP -> native N1; no host power loss or coordinated cross-service snapshot claim"
        }),
    );
}
