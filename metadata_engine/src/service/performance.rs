//! Opt-in stage timings; no instrumentation is compiled into shipped binaries.
use super::*;
use crate::{Options, object_store::ObjectStoreConfig, server::ServerConfig};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

struct Process(std::process::Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit task-owned offline M1 config, N1 binary and new output path required"]
async fn small_upload_stage_profile() {
    let path = PathBuf::from(std::env::var_os("M1_UPLOAD_PROFILE_CONFIG").unwrap());
    let output = PathBuf::from(std::env::var_os("M1_UPLOAD_PROFILE_OUTPUT").unwrap());
    assert!(!output.exists());
    let root = path.parent().unwrap();
    let config = ServerConfig::load(&path).unwrap();
    let log = std::fs::File::create(root.join("stage-object.log")).unwrap();
    let _process = Process(
        Command::new(std::env::var_os("N1_OBJECT_TEST_BINARY").unwrap())
            .args(["serve", "--config"])
            .arg(root.join("object.json"))
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap(),
    );
    let address = config.object_native_url.trim_start_matches("n1://");
    let deadline = Instant::now() + Duration::from_secs(60);
    while tokio::net::TcpStream::connect(address).await.is_err() {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let db = crate::MetadataEngine::open(&config.data_dir, Options::default()).unwrap();
    let rows = db
        .snapshot()
        .scan(
            crate::Table::Reclamation,
            PoolId([0; 16]),
            "fragment/",
            None,
            100,
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    let record: Value = serde_json::from_slice(&rows[0].1).unwrap();
    let fragment: PoolId = serde_json::from_value(record["id"].clone()).unwrap();
    let executor = MetadataExecutor::new(db, 4).unwrap();
    let objects = NativeObjectStore::new(ObjectStoreConfig {
        native_url: config.object_native_url,
        fragment: config.object_fragment,
        secret: config.object_secret,
        max_connections: 16,
        tls: None,
    })
    .unwrap();
    let prefix = Ulid::new().to_string();
    let payload = vec![0x53; 4096];
    let mut samples = Vec::new();
    for index in 0..67 {
        let mut stages = serde_json::Map::new();
        let start = Instant::now();
        let now = Instant::now();
        let upload = objects.init(None, 4096, 262144, true, 86400).await.unwrap();
        stages.insert(
            "native_begin_ms".into(),
            json!(now.elapsed().as_secs_f64() * 1000.0),
        );
        let version = Ulid::new().to_string();
        let session = Ulid::new().to_string();
        let path = format!("{prefix}-{index:03}.bin");
        let record_upload = upload.clone();
        let (v, s) = (version.clone(), session.clone());
        let created = crate::middleware::auth::now() as i64;
        let now = Instant::now();
        executor
            .execute(move |db| {
                Namespace::new(db).reserve_upload(
                    fragment,
                    &path,
                    &v,
                    &s,
                    &record_upload,
                    "application/octet-stream",
                    created,
                )
            })
            .await
            .unwrap();
        stages.insert(
            "durable_reservation_ms".into(),
            json!(now.elapsed().as_secs_f64() * 1000.0),
        );
        let (v, s) = (version.clone(), session.clone());
        let now = Instant::now();
        executor
            .execute(move |db| Namespace::new(db).pending(fragment, &v, Some(&s)))
            .await
            .unwrap();
        stages.insert(
            "pending_lookup_ms".into(),
            json!(now.elapsed().as_secs_f64() * 1000.0),
        );
        let now = Instant::now();
        let finalized = objects.put(&upload, &mut payload.as_slice()).await.unwrap();
        stages.insert(
            "native_durable_put_ms".into(),
            json!(now.elapsed().as_secs_f64() * 1000.0),
        );
        let now = Instant::now();
        let stat = objects.stat(&upload.object_key).await.unwrap();
        stages.insert(
            "native_stat_ms".into(),
            json!(now.elapsed().as_secs_f64() * 1000.0),
        );
        let now = Instant::now();
        executor
            .execute(move |db| {
                Namespace::new(db).publish(fragment, &version, &session, &finalized, &stat)
            })
            .await
            .unwrap();
        stages.insert(
            "durable_publication_ms".into(),
            json!(now.elapsed().as_secs_f64() * 1000.0),
        );
        stages.insert(
            "total_ms".into(),
            json!(start.elapsed().as_secs_f64() * 1000.0),
        );
        if index >= 3 {
            samples.push(Value::Object(stages));
        }
        let mut downloaded = Vec::new();
        objects
            .download(&upload.object_key, 0, None, &mut downloaded)
            .await
            .unwrap();
        assert_eq!(downloaded, payload);
    }
    objects.shutdown().await;
    executor.shutdown().await;
    std::fs::write(
        output,
        serde_json::to_vec_pretty(&json!({"fixture":root,"samples":samples,"verified":true}))
            .unwrap(),
    )
    .unwrap();
}
