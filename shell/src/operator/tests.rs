use super::*;
use std::{collections::BTreeMap, os::unix::fs::PermissionsExt};

fn fixture() -> (tempfile::TempDir, Arc<Host>) {
    let root = tempfile::tempdir().unwrap();
    let token = root.path().join("operator.token");
    config::generate_token(&token).unwrap();
    let host = Host::open(Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        token_file: token,
        tls_cert: None,
        tls_key: None,
        allow_plaintext: true,
        allowed_origins: vec!["https://console.example".into()],
        repositories: BTreeMap::from([(
            "primary".into(),
            config::Store {
                path: root.path().join("repository"),
                key_file: root.path().join("repository.key"),
            },
        )]),
        engine_config: None,
        engine_binary: Some("/bin/false".into()),
        restore_targets: BTreeMap::new(),
        replicas: BTreeMap::new(),
        jobs: BTreeMap::new(),
        online_source: None,
        capture_boundary_seconds: 30,
    })
    .unwrap();
    (root, host)
}

#[test]
fn operator_credentials_are_private_nonreplaceable_and_origin_scoped() {
    let (root, host) = fixture();
    let file = &host.config.token_file;
    assert!(config::generate_token(file).is_err());
    let link = root.path().join("link");
    std::os::unix::fs::symlink(file, &link).unwrap();
    assert!(config::token(&link).is_err());
    std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(config::token(file).is_err());
    let mut headers = HeaderMap::new();
    headers.insert(
        "authorization",
        format!("Bearer {}", host.secret).parse().unwrap(),
    );
    headers.insert("origin", "https://console.example".parse().unwrap());
    assert!(host.authenticate(&headers).is_ok());
    headers.insert("origin", "https://attacker.example".parse().unwrap());
    assert_eq!(host.authenticate(&headers), Err(StatusCode::FORBIDDEN));
    headers.remove("origin");
    headers.insert("authorization", "Bearer fragment-token".parse().unwrap());
    assert_eq!(host.authenticate(&headers), Err(StatusCode::UNAUTHORIZED));
}

#[tokio::test]
async fn http_operator_rejects_paths_and_unconfirmed_work_and_reports_failed_children_truthfully() {
    let (_root, host) = fixture();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://{}/noa/v1/maintenance/backups/operation",
        listener.local_addr().unwrap()
    );
    let app = router(host.clone()).unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let client = reqwest::Client::new();
    let call = |body: Value| client.post(&url).bearer_auth(&host.secret).json(&body);
    assert_eq!(
        client
            .post(&url)
            .json(&json!({"operation":"capabilities"}))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        call(json!({"operation":"capabilities"}))
            .header("origin", "https://attacker.example")
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        call(json!({"operation":"init","repository":"primary","confirm":true,"path":"/arbitrary"}))
            .send()
            .await
            .unwrap()
            .status(),
        422
    );
    assert_eq!(
        call(json!({"operation":"init","repository":"primary"}))
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert_eq!(
        call(json!({"operation":"init","repository":"unknown","confirm":true}))
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert!(host.jobs.lock().unwrap().is_empty());
    assert!(!host.config.repositories["primary"].path.exists());
    let response = call(json!({"operation":"init","repository":"primary","confirm":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    drop(response); // accepted job outlives the reply
    host.drain().await;
    let jobs = host.jobs.lock().unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].state, "failed");
    assert!(jobs[0].result.is_none());
    assert!(
        !serde_json::to_string(&*jobs)
            .unwrap()
            .contains(&host.secret)
    );
    server.abort();
}

#[tokio::test]
async fn recovery_inventory_preserves_descending_keyset_pages_from_real_authenticated_manifests() {
    use n1::core::{
        Store,
        backup::{capture, repository::Repository},
        storage_pool::Pool,
    };
    use n1_link::{PoolId, PoolLayout, ShardFormat};
    let (root, host) = fixture();
    let configured = &host.config.repositories["primary"];
    n1::core::backup::create_key(&configured.key_file).unwrap();
    Repository::initialize(&configured.path, &configured.key_file).unwrap();
    let member = root.path().join("member");
    std::fs::create_dir(&member).unwrap();
    let pool = Pool::enroll(
        PoolLayout {
            id: PoolId([7; 16]),
            generation: 1,
            data_shards: 1,
            parity_shards: 0,
            block_bytes: 4096,
            placement_version: 1,
            shard_format: ShardFormat::Crc32cSha256V2,
        },
        &[member],
    )
    .unwrap();
    let store = Store::new(vec![pool], [3; 32], 4).unwrap();
    let mut expected = Vec::new();
    for _ in 0..3 {
        let repo = Repository::open(&configured.path, &configured.key_file, true).unwrap();
        expected.push(capture::offline(&repo, &store).unwrap().id);
    }
    expected.sort();
    expected.reverse();
    let first = host
        .dispatch(Request {
            operation: "list".into(),
            repository: "primary".into(),
            limit: 2,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(first["has_more"], true);
    assert_eq!(first["snapshots"][0]["snapshot"], expected[0]);
    assert_eq!(first["snapshots"][1]["snapshot"], expected[1]);
    let second = host
        .dispatch(Request {
            operation: "list".into(),
            repository: "primary".into(),
            limit: 2,
            cursor: first["cursor"].as_str().unwrap().into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(second["has_more"], false);
    assert_eq!(second["snapshots"].as_array().unwrap().len(), 1);
    assert_eq!(second["snapshots"][0]["snapshot"], expected[2]);
}
