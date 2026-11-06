//! Cross-process witness using the unchanged object binary. All payloads,
//! metadata, configuration and logs live under explicit test HDD directories.
use m1::{
    MetadataEngine, MetadataExecutor, Options, RecordKey, Table,
    namespace::Namespace,
    object_store::{NativeObjectStore, ObjectStoreConfig, ObjectUpload},
    service::{MetadataService, UploadRequest},
};
use n1_link::{PoolId, noa::Command};
use n1_native_client::N1NativeClient;
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    process::{Child, Command as Process, Stdio},
    time::Duration,
};
use tokio::io::AsyncWriteExt;

const FRAGMENT: PoolId = PoolId([91; 16]);
struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn client(credentials: &Value) -> NativeObjectStore {
    NativeObjectStore::new(ObjectStoreConfig {
        native_url: credentials["native_url"].as_str().unwrap().into(),
        fragment: credentials["fragment"].as_str().unwrap().into(),
        secret: credentials["secret"].as_str().unwrap().into(),
        max_connections: 4,
        tls: None,
    })
    .unwrap()
}

fn serve(binary: &Path, config: &Path, root: &Path) -> Server {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join("object-server.log"))
        .unwrap();
    Server(
        Process::new(binary)
            .args(["serve", "--config"])
            .arg(config)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap(),
    )
}

async fn ready(credentials: &Value) {
    for _ in 0..100 {
        if N1NativeClient::connect(credentials["native_url"].as_str().unwrap())
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("object process failed to start");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "physical witness: requires N1_OBJECT_TEST_BINARY and N1_OBJECT_TEST_ROOT on an HDD; optionally N1_OBJECT_TEST_SECOND_ROOT on a different HDD"]
async fn metadata_uses_unchanged_link_and_link_survives_without_metadata() {
    let binary = PathBuf::from(
        std::env::var_os("N1_OBJECT_TEST_BINARY").expect("explicit tested object binary required"),
    );
    let parent =
        PathBuf::from(std::env::var_os("N1_OBJECT_TEST_ROOT").expect("explicit HDD root required"));
    let id = format!("native-metadata-{}", ulid::Ulid::new());
    let root = parent.join(&id);
    std::fs::create_dir(&root).unwrap();
    let first = root.join("member-0");
    std::fs::create_dir(&first).unwrap();
    let second =
        std::env::var_os("N1_OBJECT_TEST_SECOND_ROOT").map(|path| PathBuf::from(path).join(&id));
    if let Some(path) = &second {
        std::fs::create_dir(path).unwrap();
    }
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);
    let config = root.join("object.json");
    let mut init = Process::new(&binary);
    init.args(["init", "--config"])
        .arg(&config)
        .arg("--member")
        .arg(&first);
    if let Some(second) = &second {
        init.arg("--member").arg(second);
    }
    let output = init
        .args([
            "--data-shards",
            "1",
            "--parity-shards",
            if second.is_some() { "1" } else { "0" },
            "--block-bytes",
            "4096",
            "--native-address",
        ])
        .arg(address.to_string())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let credentials: Value =
        serde_json::from_slice(&std::fs::read(config.with_extension("credentials.json")).unwrap())
            .unwrap();
    let mut server = serve(&binary, &config, &root);
    ready(&credentials).await;

    // A direct object client needs no metadata process or database.
    let direct = client(&credentials);
    let bytes = (0..131_195).map(|n| (n % 251) as u8).collect::<Vec<_>>();
    let direct_upload = direct
        .init(None, bytes.len() as u64, 4096 * 33, true, 3600)
        .await
        .unwrap();
    direct
        .put(&direct_upload, &mut bytes.as_slice())
        .await
        .unwrap();
    let mut read = Vec::new();
    direct
        .download(&direct_upload.object_key, 0, None, &mut read)
        .await
        .unwrap();
    assert_eq!(read, bytes);

    let db_path = root.join("metadata");
    let db = MetadataEngine::open(&db_path, Options::default()).unwrap();
    let namespace = Namespace::new(db.clone());
    let inspect = db.clone();
    let backend = client(&credentials);
    let service = MetadataService::new(MetadataExecutor::new(db, 8).unwrap(), backend.clone());
    let request = |path: &str, length: usize, part_bytes: u64| UploadRequest {
        fragment: FRAGMENT,
        path: path.into(),
        content_type: "application/octet-stream".into(),
        pool: None,
        total_bytes: length as u64,
        part_bytes,
        durable: true,
        ttl_seconds: 3600,
    };

    let handle = service
        .begin_upload(request("before/file", bytes.len(), 4096 * 33))
        .await
        .unwrap();
    let status = service
        .upload_status(FRAGMENT, &handle.version_id, 0, 100)
        .await
        .unwrap();
    assert_eq!(status.missing_parts, vec![0]);
    let published = service
        .put(
            FRAGMENT,
            &handle.version_id,
            &handle.session_id,
            &mut bytes.as_slice(),
        )
        .await
        .unwrap();
    let pending = service
        .begin_upload(request("before/file", 8_201, 4096))
        .await
        .unwrap();
    namespace
        .rename(FRAGMENT, "before/file", "after/file")
        .unwrap();
    for part in [2u64, 0, 1] {
        let start = part as usize * 4096;
        let end = (start + 4096).min(8_201);
        service
            .upload_part(
                FRAGMENT,
                &pending.version_id,
                part,
                (end - start) as u64,
                &mut bytes[start..end].as_ref(),
            )
            .await
            .unwrap();
    }
    let status = service
        .upload_status(FRAGMENT, &pending.version_id, 0, 100)
        .await
        .unwrap();
    assert!(status.missing_parts.is_empty());
    // Exercise the critical lost-publication boundary: LINK is durably complete
    // while the logical head still refers to the older committed version.
    let saved: Value = serde_json::from_slice(
        &inspect
            .snapshot()
            .get(
                Table::Uploads,
                &RecordKey::new(FRAGMENT, &pending.version_id).unwrap(),
            )
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let ticket: ObjectUpload = serde_json::from_value(saved["upload"].clone()).unwrap();
    backend.finalize(&ticket).await.unwrap();
    assert_eq!(
        namespace
            .stat(FRAGMENT, "after/file", None)
            .unwrap()
            .version_id,
        published.version_id
    );
    drop(inspect);
    // Restart both components with an unfinished metadata transaction. The
    // durable LINK ticket and metadata session retain the same final identity.
    service.shutdown().await;
    drop(service);
    drop(namespace);
    server.0.kill().unwrap();
    server.0.wait().unwrap();
    drop(server);
    server = serve(&binary, &config, &root);
    ready(&credentials).await;
    let db = MetadataEngine::open(&db_path, Options::default()).unwrap();
    let namespace = Namespace::new(db.clone());
    let service = MetadataService::new(MetadataExecutor::new(db, 8).unwrap(), client(&credentials));
    let newer = service
        .finalize(FRAGMENT, &pending.version_id, &pending.session_id)
        .await
        .unwrap();
    let repeated = service
        .finalize(FRAGMENT, &pending.version_id, &pending.session_id)
        .await
        .unwrap();
    assert_eq!(newer, repeated);
    assert_eq!(newer.object_id, published.object_id);
    let mut latest = Vec::new();
    service
        .download(FRAGMENT, "after/file", None, 0, None, &mut latest)
        .await
        .unwrap();
    assert_eq!(latest, bytes[..8_201]);
    let mut older = Vec::new();
    service
        .download(
            FRAGMENT,
            "after/file",
            Some(&published.version_id),
            0,
            None,
            &mut older,
        )
        .await
        .unwrap();
    assert_eq!(older, bytes);
    let trash = namespace.trash(FRAGMENT, "after/file", 1700000000).unwrap();
    namespace.recover(FRAGMENT, &trash).unwrap();
    let empty = service
        .begin_upload(request("empty", 0, 4096))
        .await
        .unwrap();
    service
        .finalize(FRAGMENT, &empty.version_id, &empty.session_id)
        .await
        .unwrap();
    let mut none = Vec::new();
    assert_eq!(
        service
            .download(FRAGMENT, "empty", None, 0, None, &mut none)
            .await
            .unwrap(),
        0
    );

    // Remove the optional metadata service from the running configuration: the
    // independent object service and original client-held key continue working.
    service.shutdown().await;
    drop(service);
    drop(namespace);
    drop(backend);
    direct.shutdown().await;
    drop(direct);
    let independent = client(&credentials);
    let mut after_metadata = Vec::new();
    independent
        .download(&direct_upload.object_key, 0, None, &mut after_metadata)
        .await
        .unwrap();
    assert_eq!(after_metadata, bytes);
    let mut raw = N1NativeClient::connect(credentials["native_url"].as_str().unwrap())
        .await
        .unwrap();
    raw.auth_fragment(
        credentials["fragment"].as_str().unwrap(),
        credentials["secret"].as_str().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        raw.control(Command::Ping).await.unwrap().result.unwrap()["placement"],
        "LINK-v1"
    );
    assert!(!first.join("metadata").exists());
    assert!(!first.join("n1_entries").exists());
    independent.shutdown().await;
    drop(independent);
    drop(raw);
    drop(server);
    let mut evidence = tokio::fs::File::create(root.join("PASSED.json"))
        .await
        .unwrap();
    evidence.write_all(serde_json::to_string_pretty(&serde_json::json!({
        "result": "passed",
        "object_binary": binary,
        "members": [Some(first),second],
        "metadata": db_path,
        "verified_bytes": bytes.len(),
        "checks": ["standalone LINK without metadata", "native metadata upload", "out-of-order multipart", "rename with pending upload", "cross-process restart", "finalize retry", "immutable history", "trash/recover", "empty object", "standalone LINK after metadata shutdown"]
    })).unwrap().as_bytes()).await.unwrap();
    evidence.sync_all().await.unwrap();
    println!("evidence: {}", root.display());
}
