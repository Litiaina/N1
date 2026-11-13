//! Real socket tests, with RAM-backed functional members. The product never
//! relaxes its independent-filesystem enrollment guard for these fixtures.
#[path = "tests/body.rs"]
mod body;
use crate::{
    core::tests::Fixture,
    middleware::auth_middleware::Auth,
    protocol::ops::Service,
    settings::config::{Config, ConfiguredPool, Fragment, Tls},
};
use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
use n1_link::{
    PoolId,
    noa::{Command, Permissions, Request},
};
use n1_native_client::{N1NativeClient, NativeClientError};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::net::TcpListener;
const SECRET: &str = "functional-test-secret";
struct Server {
    fixture: Fixture,
    service: Arc<Service>,
    native: String,
    http: String,
    handles: Vec<tokio::task::JoinHandle<()>>,
}
impl Drop for Server {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}
impl Server {
    async fn new(tls: bool) -> Self {
        Self::new_recovery(tls, false).await
    }
    async fn new_recovery(tls: bool, recovery: bool) -> Self {
        Self::new_options(tls, recovery, None).await
    }
    async fn new_options(
        tls: bool,
        recovery: bool,
        rates: Option<crate::middleware::rate_limiter_middleware::RateLimitSettings>,
    ) -> Self {
        let fixture = Fixture::new();
        let store = fixture.enroll();
        let repositories = if recovery {
            let key_file = fixture.base.join("recovery.key");
            crate::core::backup::create_key(&key_file).unwrap();
            let path = fixture.base.join("recovery");
            crate::core::backup::repository::Repository::initialize(&path, &key_file).unwrap();
            std::collections::BTreeMap::from([(
                "archive".into(),
                crate::core::backup::network::ConfiguredRepository { path, key_file },
            )])
        } else {
            Default::default()
        };
        let hash = Argon2::default()
            .hash_password(
                SECRET.as_bytes(),
                &SaltString::encode_b64(&[19; 16]).unwrap(),
            )
            .unwrap()
            .to_string();
        let all = Permissions::all();
        let fragments = vec![
            Fragment {
                id: PoolId([11; 16]),
                alias: "test".into(),
                secret_hash: hash.clone(),
                permissions: all,
                operator: true,
            },
            Fragment {
                id: PoolId([12; 16]),
                alias: "other".into(),
                secret_hash: hash,
                permissions: all,
                operator: false,
            },
        ];
        let native = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let native_address = native.local_addr().unwrap();
        let http_address = http.local_addr().unwrap();
        let tls = if tls {
            let certificate = fixture.base.join("cert.pem");
            let private_key = fixture.base.join("key.pem");
            let output = std::process::Command::new("openssl")
                .args([
                    "req",
                    "-x509",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-days",
                    "1",
                    "-subj",
                    "/CN=localhost",
                    "-addext",
                    "subjectAltName=DNS:localhost,IP:127.0.0.1",
                    "-addext",
                    "basicConstraints=critical,CA:FALSE",
                    "-keyout",
                ])
                .arg(&private_key)
                .arg("-out")
                .arg(&certificate)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            Some(Tls {
                certificate,
                private_key,
            })
        } else {
            None
        };
        let config = Config {
            format: crate::settings::config::CONFIG_FORMAT.into(),
            native_address,
            http_address: Some(http_address),
            signing_key_file: fixture.base.join("unused-key"),
            state_dir: None,
            max_connections: 16,
            max_operations: 16,
            operation_memory_bytes: 256 * 1024 * 1024,
            read_repair: false,
            concurrency: Default::default(),
            rate_limits: rates.unwrap_or(
                crate::middleware::rate_limiter_middleware::RateLimitSettings {
                    enabled: false,
                    ..Default::default()
                },
            ),
            repositories,
            maintenance: crate::core::maintenance::jobs::MaintenanceConfig::default(),
            runtime: Default::default(),
            tls,
            pools: vec![ConfiguredPool {
                layout: fixture.layout.clone(),
                members: store.pools[0].members[0]
                    .as_ref()
                    .unwrap()
                    .marker
                    .members
                    .clone(),
                roots: fixture.paths.clone(),
                sealed: false,
            }],
            fragments: fragments.clone(),
        };
        let service = Arc::new(Service {
            _control_lease: None,
            limits: crate::middleware::rate_limiter_middleware::RateLimits::new(config.rate_limits),
            config,
            store,
            recovery_jobs: crate::core::backup::jobs::Jobs::new(),
            auth: Auth::with_session_root(fragments, &[17; 32], fixture.base.join("sessions")),
            jobs: crate::core::maintenance::jobs::Jobs::new(),
            transfers: Arc::new(tokio::sync::Semaphore::new(2)),
            observations: Arc::default(),
        });
        let engine = service.clone();
        let native_handle = tokio::spawn(async move {
            super::serve(native, engine).await.unwrap();
        });
        let router = crate::routes::storage_routes::router(service.clone());
        let http_handle = tokio::spawn(async move {
            axum::serve(http, router).await.unwrap();
        });
        Self {
            native: format!(
                "{}://localhost:{}",
                if service.config.tls.is_some() {
                    "n1s"
                } else {
                    "n1"
                },
                native_address.port()
            ),
            http: format!("http://{http_address}"),
            fixture,
            service,
            handles: vec![native_handle, http_handle],
        }
    }
    async fn client(&self) -> N1NativeClient {
        let mut client = N1NativeClient::connect(&self.native).await.unwrap();
        client.auth_fragment("test", SECRET).await.unwrap();
        client
    }
}
async fn init(client: &mut N1NativeClient, total: u64) -> Value {
    client
        .control(Command::UploadInit {
            pool: None,
            total_bytes: total,
            part_bytes: 24576,
            durable: true,
            ttl_seconds: 3600,
        })
        .await
        .unwrap()
        .result
        .unwrap()
}

#[tokio::test]
async fn allocation_and_object_io_do_not_wait_for_a_stalled_monitoring_probe() {
    use std::time::Duration;
    let server = Server::new(false).await;
    let mut client = server.client().await;
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let cache = server.service.observations.clone();
    let ready = started.notified();
    let entered = started.clone();
    let finish = release.clone();
    let collector = tokio::spawn(async move {
        cache
            .observe(Duration::from_secs(300), move || async move {
                entered.notify_one();
                let _permit = finish.acquire().await.unwrap();
                json!({"status":"healthy"})
            })
            .await
    });
    ready.await;
    let response = tokio::time::timeout(Duration::from_secs(1), client.control(Command::Pools))
        .await
        .expect("placement discovery waited for monitoring")
        .unwrap();
    let pools = response.result.unwrap();
    assert_eq!(pools["pools"][0]["id"], json!(server.fixture.layout.id));
    assert_eq!(pools["pools"][0]["available_members"], 4);
    let payload = vec![37u8; 12288];
    tokio::time::timeout(Duration::from_secs(1), async {
        let plan = init(&mut client, payload.len() as u64).await;
        client
            .put(
                plan["object_key"].as_str().unwrap().into(),
                payload.len() as u64,
                &mut std::io::Cursor::new(payload.clone()),
            )
            .await
            .unwrap();
        let mut read = Vec::new();
        client
            .download(
                plan["object_key"].as_str().unwrap().into(),
                0,
                None,
                &mut read,
            )
            .await
            .unwrap();
        assert_eq!(read, payload);
    })
    .await
    .expect("object I/O waited for monitoring");
    release.add_permits(1);
    assert_eq!(collector.await.unwrap().sequence, 1);
}
fn string(v: &Value, name: &str) -> String {
    v[name].as_str().unwrap().into()
}
fn is_status(result: Result<n1_link::noa::Response, NativeClientError>, status: &str) {
    assert!(matches!(result,Err(NativeClientError::Native{code,..})if code==status));
}

#[tokio::test]
async fn configuration_identity_is_stable_and_ping_reports_the_binary_release() {
    let server = Server::new(false).await;
    let mut config = server.service.config.clone();
    config.validate().unwrap();
    assert_eq!(serde_json::to_value(&config).unwrap()["format"], "N1-LINK");
    config.format = format!("N1-LINK-{}", env!("CARGO_PKG_VERSION"));
    assert!(
        config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("unsupported N1 configuration format")
    );
    config.format = crate::settings::config::CONFIG_FORMAT.into();
    config.pools.clear();
    assert!(
        config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("at least one storage pool and fragment")
    );

    let mut native = N1NativeClient::connect(&server.native).await.unwrap();
    let ping = native.control(Command::Ping).await.unwrap().result.unwrap();
    assert_eq!(ping["version"], env!("CARGO_PKG_VERSION"));
    let http: n1_link::noa::Response = reqwest::Client::new()
        .post(format!("{}/noa", server.http))
        .json(&Request {
            id: 1,
            body_len: 0,
            command: Command::Ping,
        })
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(http.result.unwrap(), ping);
}

#[tokio::test]
async fn refresh_rotation_reuse_and_logout_work_across_native_and_http() {
    let server = Server::new(false).await;
    let mut client = N1NativeClient::connect(&server.native).await.unwrap();
    let initial = client
        .auth_fragment("test", SECRET)
        .await
        .unwrap()
        .result
        .unwrap();
    let refresh = string(&initial, "refresh_token");
    let rotated: n1_link::noa::Response = reqwest::Client::new()
        .post(format!("{}/noa", server.http))
        .json(&Request {
            id: 1,
            body_len: 0,
            command: Command::Refresh {
                refresh_token: refresh.clone(),
            },
        })
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let rotated = rotated.result.unwrap();
    assert_ne!(refresh, string(&rotated, "refresh_token"));
    is_status(
        client
            .control(Command::Refresh {
                refresh_token: refresh,
            })
            .await,
        "401",
    );
    is_status(
        client
            .control(Command::Refresh {
                refresh_token: string(&rotated, "refresh_token"),
            })
            .await,
        "401",
    );
    let initial = client
        .auth_fragment("test", SECRET)
        .await
        .unwrap()
        .result
        .unwrap();
    client.control(Command::Logout).await.unwrap();
    is_status(
        client
            .control(Command::Refresh {
                refresh_token: string(&initial, "refresh_token"),
            })
            .await,
        "401",
    );
}

#[tokio::test]
async fn native_and_http_share_authentication_and_fragment_rate_quotas() {
    use crate::middleware::rate_limiter_middleware::RateLimitSettings;
    let server = Server::new_options(
        false,
        false,
        Some(RateLimitSettings {
            auth_per_sec: 1,
            auth_burst: 2,
            global_per_sec: 5000,
            global_burst: 10000,
            ..Default::default()
        }),
    )
    .await;
    let mut client = N1NativeClient::connect(&server.native).await.unwrap();
    for _ in 0..2 {
        is_status(client.auth_bearer("invalid".into()).await, "401");
    }
    let response = reqwest::Client::new()
        .post(format!("{}/noa", server.http))
        .json(&Request {
            id: 1,
            body_len: 0,
            command: Command::Bearer {
                token: "invalid".into(),
            },
        })
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 429);

    let server = Server::new_options(
        false,
        false,
        Some(RateLimitSettings {
            auth_per_sec: 100,
            auth_burst: 100,
            view_per_sec: 1,
            view_burst: 1,
            ..Default::default()
        }),
    )
    .await;
    let mut client = N1NativeClient::connect(&server.native).await.unwrap();
    let login = client
        .auth_fragment("test", SECRET)
        .await
        .unwrap()
        .result
        .unwrap();
    client.control(Command::Pools).await.unwrap();
    let http = reqwest::Client::new();
    let request = Request {
        id: 2,
        body_len: 0,
        command: Command::Pools,
    };
    assert_eq!(
        http.post(format!("{}/noa", server.http))
            .bearer_auth(string(&login, "token"))
            .json(&request)
            .send()
            .await
            .unwrap()
            .status(),
        429
    );
    client.auth_fragment("other", SECRET).await.unwrap();
    client.control(Command::Pools).await.unwrap();
}

#[tokio::test]
async fn signed_downloads_preserve_scope_ranges_integrity_and_purged_absence() {
    let server = Server::new(false).await;
    let mut owner = server.client().await;
    let data = b"immutable signed object data".to_vec();
    let upload = init(&mut owner, data.len() as u64).await;
    let key = string(&upload, "object_key");
    owner
        .put(
            string(&upload, "object_key"),
            data.len() as u64,
            &mut &data[..],
        )
        .await
        .unwrap();
    let grant: n1_link::noa::ReadGrant = serde_json::from_value(
        owner
            .control(Command::SignedRead {
                object_key: key.clone(),
                offset: 3,
                length: Some(12),
                ttl_seconds: 60,
            })
            .await
            .unwrap()
            .result
            .unwrap(),
    )
    .unwrap();
    let mut anonymous = N1NativeClient::connect(&server.native).await.unwrap();
    let mut output = Vec::new();
    anonymous
        .download_signed(grant.clone(), &mut output)
        .await
        .unwrap();
    assert_eq!(output, data[3..15]);
    is_status(
        anonymous
            .control(Command::ObjectStat {
                object_key: key.clone(),
            })
            .await,
        "401",
    );
    let http = reqwest::Client::new();
    let response = http
        .get(format!("{}/noa/signed", server.http))
        .query(&grant)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 206);
    assert_eq!(response.bytes().await.unwrap().as_ref(), &data[3..15]);
    let mut changed = grant.clone();
    changed.offset += 1;
    assert!(
        matches!(anonymous.download_signed(changed, &mut Vec::new()).await, Err(NativeClientError::Native {code,..}) if code == "401")
    );
    owner
        .control(Command::ObjectDelete { object_key: key })
        .await
        .unwrap();
    assert!(
        matches!(anonymous.download_signed(grant, &mut Vec::new()).await, Err(NativeClientError::Native {code,..}) if code == "404")
    );
}

#[tokio::test]
async fn http_download_ranges_and_preconditions_preserve_immutable_bytes_and_signed_bounds() {
    let server = Server::new(false).await;
    let mut client = server.client().await;
    let login = client.auth_fragment("test", SECRET).await.unwrap();
    let token = string(&login.result.unwrap(), "token");
    let data = b"0123456789";
    let upload = init(&mut client, data.len() as u64).await;
    client
        .put(
            string(&upload, "object_key"),
            data.len() as u64,
            &mut &data[..],
        )
        .await
        .unwrap();
    let key = string(&upload, "object_key");
    let url = format!("{}/noa/object/{key}", server.http);
    let http = reqwest::Client::new();
    let response = http.head(&url).bearer_auth(&token).send().await.unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["content-length"], "10");
    assert_eq!(response.headers()["accept-ranges"], "bytes");
    assert_eq!(response.headers()["cache-control"], "private, no-store");
    let etag = response.headers()["etag"].to_str().unwrap().to_owned();
    assert!(response.bytes().await.unwrap().is_empty());
    for (requested, expected, content_range) in [
        ("bytes=2-5", &data[2..6], "bytes 2-5/10"),
        ("BYTES=7-", &data[7..], "bytes 7-9/10"),
        ("bytes=-3", &data[7..], "bytes 7-9/10"),
        ("bytes=-100", &data[..], "bytes 0-9/10"),
        (
            "bytes=0-99999999999999999999999999999",
            &data[..],
            "bytes 0-9/10",
        ),
        (
            "bytes=-99999999999999999999999999999",
            &data[..],
            "bytes 0-9/10",
        ),
    ] {
        let response = http
            .get(&url)
            .bearer_auth(&token)
            .header("range", requested)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 206, "{requested}");
        assert_eq!(response.headers()["content-range"], content_range);
        assert_eq!(response.headers()["etag"], etag);
        assert_eq!(response.bytes().await.unwrap().as_ref(), expected);
    }
    for invalid in [
        "bytes=10-",
        "bytes=2",
        "bytes=+2-5",
        "bytes=7-3",
        "bytes=-0",
        "bytes=0-1,3-4",
        "bytes=99999999999999999999999999-",
    ] {
        let response = http
            .get(&url)
            .bearer_auth(&token)
            .header("range", invalid)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 416, "{invalid}");
        assert_eq!(response.headers()["content-range"], "bytes */10");
        assert!(response.bytes().await.unwrap().is_empty());
    }
    for (validator, expected_status, expected) in [
        (etag.clone(), 206, &data[2..6]),
        (format!("W/{etag}"), 200, &data[..]),
        ("\"different\"".into(), 200, &data[..]),
        ("Wed, 21 Oct 2015 07:28:00 GMT".into(), 200, &data[..]),
    ] {
        let response = http
            .get(&url)
            .bearer_auth(&token)
            .header("range", "bytes=2-5")
            .header("if-range", validator)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected_status);
        assert_eq!(response.bytes().await.unwrap().as_ref(), expected);
    }
    let response = http
        .get(&url)
        .bearer_auth(&token)
        .header("if-match", format!("\"other\", {etag}"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.bytes().await.unwrap().as_ref(), data);
    for method in [reqwest::Method::GET, reqwest::Method::HEAD] {
        let response = http
            .request(method.clone(), &url)
            .bearer_auth(&token)
            .header("if-none-match", format!("\"other\", W/{etag}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 304);
        assert_eq!(response.headers()["etag"], etag);
        assert_eq!(response.headers()["cache-control"], "private, no-store");
        assert!(response.bytes().await.unwrap().is_empty());
        let response = http
            .request(method, &url)
            .bearer_auth(&token)
            .header("if-match", format!("W/{etag}"))
            .header("if-none-match", "*")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 412);
        assert!(response.bytes().await.unwrap().is_empty());
    }
    let grant: n1_link::noa::ReadGrant = serde_json::from_value(
        client
            .control(Command::SignedRead {
                object_key: key,
                offset: 2,
                length: Some(4),
                ttl_seconds: 60,
            })
            .await
            .unwrap()
            .result
            .unwrap(),
    )
    .unwrap();
    let response = http
        .get(format!("{}/noa/signed", server.http))
        .query(&grant)
        .header("range", "bytes=0-9")
        .header("if-range", "\"different\"")
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        400,
        "If-Range cannot widen a signed grant"
    );
    let response = http
        .get(format!("{}/noa/signed", server.http))
        .query(&grant)
        .header("if-range", "\"different\"")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 206);
    assert_eq!(response.bytes().await.unwrap().as_ref(), &data[2..6]);

    let empty = init(&mut client, 0).await;
    client
        .control(Command::UploadFinalize {
            object_key: string(&empty, "object_key"),
        })
        .await
        .unwrap();
    let response = http
        .get(format!(
            "{}/noa/object/{}",
            server.http,
            string(&empty, "object_key")
        ))
        .bearer_auth(token)
        .header("range", "bytes=-100")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["content-length"], "0");
    assert!(response.headers().get("content-range").is_none());
    assert!(response.bytes().await.unwrap().is_empty());
}

#[tokio::test]
async fn maintenance_modes_gate_real_requests_and_are_independent_between_stores() {
    use crate::core::operation_gate::MaintenanceAccessMode;
    let server = Server::new(false).await;
    let mut client = server.client().await;
    let upload = init(&mut client, 4).await;
    let key = string(&upload, "object_key");
    client
        .put(string(&upload, "object_key"), 4, &mut &b"data"[..])
        .await
        .unwrap();
    let read_only = server
        .service
        .store
        .operation_gate
        .acquire(MaintenanceAccessMode::ReadOnly);
    client
        .control(Command::ObjectStat {
            object_key: key.clone(),
        })
        .await
        .unwrap();
    is_status(
        client
            .control(Command::ObjectDelete {
                object_key: key.clone(),
            })
            .await,
        "503",
    );
    let quiesced = server
        .service
        .store
        .operation_gate
        .acquire(MaintenanceAccessMode::Quiesced);
    is_status(
        client
            .control(Command::ObjectStat {
                object_key: key.clone(),
            })
            .await,
        "503",
    );
    assert!(
        matches!(client.download(key.clone(), 0, None, &mut Vec::new()).await, Err(NativeClientError::Native {code,..}) if code == "503")
    );
    let other = crate::core::tests::Fixture::new();
    other
        .enroll()
        .init([11; 16], 0, 0, 12288, n1_link::Durability::Durable, 60)
        .await
        .unwrap();
    drop(quiesced);
    drop(read_only);
    client
        .control(Command::ObjectDelete { object_key: key })
        .await
        .unwrap();
}

#[tokio::test]
async fn retained_backup_supervisor_observes_completed_native_and_http_capture_jobs() {
    use crate::core::backup::network::{
        Destination, Request as BackupRequest, completed_operation,
    };
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let server = Server::new_recovery(false, true).await;
    let credentials = server.fixture.base.join("job-credentials.json");
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&credentials)
        .unwrap();
    file.write_all(&serde_json::to_vec(&json!({"fragment":"test","secret":SECRET})).unwrap())
        .unwrap();
    for url in [&server.native, &server.http] {
        let value = completed_operation(
            &Destination {
                url: url.clone(),
                repository: "archive".into(),
                token_file: credentials.clone(),
                ca_file: None,
                allow_plaintext: true,
            },
            BackupRequest {
                operation: "create-online".into(),
                repository: "archive".into(),
                confirm: true,
                ..Default::default()
            },
            15,
        )
        .await
        .unwrap();
        assert_eq!(value["capture_mode"], "online_committed");
        assert!(value["id"].as_str().unwrap().parse::<ulid::Ulid>().is_ok());
    }
}

#[tokio::test]
async fn native_streams_resume_conflicts_ranges_and_permanent_delete() {
    let server = Server::new(false).await;
    let mut client = server.client().await;
    let data = (0..70001)
        .map(|i| (i * 197 % 251) as u8)
        .collect::<Vec<_>>();
    let upload = init(&mut client, data.len() as u64).await;
    let key = string(&upload, "object_key");
    for part in [2, 0] {
        let start = part * 24576;
        let end = (start + 24576).min(data.len());
        client
            .write_at(
                key.clone(),
                start as u64,
                (end - start) as u64,
                &mut std::io::Cursor::new(&data[start..end]),
            )
            .await
            .unwrap();
    }
    let status = client
        .control(Command::UploadStatus {
            object_key: key.clone(),
            start: 0,
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(status.result.unwrap()["missing_parts"], json!([1]));
    let mut changed = data[..24576].to_vec();
    changed[1] ^= 1;
    is_status(
        client
            .write_at(key.clone(), 0, 24576, &mut std::io::Cursor::new(changed))
            .await,
        "409",
    );
    // The rejected body was fully drained: the same connection stays aligned.
    client
        .write_at(
            key.clone(),
            24576,
            24576,
            &mut std::io::Cursor::new(&data[24576..49152]),
        )
        .await
        .unwrap();
    client
        .control(Command::UploadFinalize {
            object_key: key.clone(),
        })
        .await
        .unwrap();
    assert_eq!(
        client
            .control(Command::UploadFinalize {
                object_key: key.clone()
            })
            .await
            .unwrap()
            .result
            .unwrap()["already_finalized"],
        true
    );
    let mut bytes = Vec::new();
    let (_, length) = client
        .download(key.clone(), 4095, Some(60001), &mut bytes)
        .await
        .unwrap();
    assert_eq!(length, 60001);
    assert_eq!(bytes, data[4095..64096]);
    client
        .control(Command::ObjectDelete {
            object_key: key.clone(),
        })
        .await
        .unwrap();
    is_status(
        client
            .control(Command::ObjectStat {
                object_key: key.clone(),
            })
            .await,
        "404",
    );
    is_status(
        client
            .control(Command::UploadFinalize { object_key: key })
            .await,
        "404",
    );
}

#[tokio::test]
async fn fragment_scope_and_requested_permissions_apply_to_both_transports() {
    let server = Server::new(false).await;
    let mut client = server.client().await;
    let upload = init(&mut client, 0).await;
    client
        .control(Command::UploadFinalize {
            object_key: string(&upload, "object_key"),
        })
        .await
        .unwrap();
    let key = string(&upload, "object_key");
    let mut other = N1NativeClient::connect(&server.native).await.unwrap();
    other.auth_fragment("other", SECRET).await.unwrap();
    assert!(
        other
            .control(Command::ObjectStat {
                object_key: key.clone()
            })
            .await
            .is_err()
    );
    let response = client
        .control(Command::Login {
            fragment: "test".into(),
            secret: SECRET.into(),
            permissions: Some(Permissions {
                read: true,
                ..Permissions::default()
            }),
            ttl_seconds: Some(60),
        })
        .await
        .unwrap();
    let token = string(&response.result.unwrap(), "token");
    is_status(
        client
            .control(Command::ObjectDelete {
                object_key: key.clone(),
            })
            .await,
        "403",
    );
    let http = reqwest::Client::new();
    let response = http
        .get(format!("{}/noa/object/{key}", server.http))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(response.bytes().await.unwrap().is_empty());
    let response = http
        .post(format!("{}/noa", server.http))
        .bearer_auth(&token)
        .json(&Request {
            id: 1,
            body_len: 0,
            command: Command::ObjectDelete { object_key: key },
        })
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 403);
    assert_eq!(
        http.get(format!("{}/noa/object/invalid", server.http))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
}

#[tokio::test]
async fn incomplete_client_upload_poisoned_connection_can_resume_on_new_connection() {
    let server = Server::new(false).await;
    let mut client = server.client().await;
    let upload = init(&mut client, 24576).await;
    let key = string(&upload, "object_key");
    assert!(
        client
            .write_at(
                key.clone(),
                0,
                24576,
                &mut std::io::Cursor::new(vec![7; 13000])
            )
            .await
            .is_err()
    );
    assert!(matches!(
        client.control(Command::Ping).await,
        Err(NativeClientError::Protocol(_))
    ));
    drop(client);
    let mut client = server.client().await;
    let status = client
        .control(Command::UploadStatus {
            object_key: key.clone(),
            start: 0,
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(status.result.unwrap()["missing_parts"], json!([0]));
    client
        .write_at(
            key.clone(),
            0,
            24576,
            &mut std::io::Cursor::new(vec![7; 24576]),
        )
        .await
        .unwrap();
    client
        .control(Command::UploadFinalize { object_key: key })
        .await
        .unwrap();
    let mut bytes = Vec::new();
    client
        .download(string(&upload, "object_key"), 0, None, &mut bytes)
        .await
        .unwrap();
    assert_eq!(bytes, vec![7; 24576]);
}

#[tokio::test]
async fn native_tls_accepts_trusted_certificate_and_rejects_untrusted_certificate() {
    use rustls::pki_types::{CertificateDer, pem::PemObject};
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = Server::new(true).await;
    let mut roots = rustls::RootCertStore::empty();
    for cert in
        CertificateDer::pem_file_iter(&server.service.config.tls.as_ref().unwrap().certificate)
            .unwrap()
    {
        roots.add(cert.unwrap()).unwrap();
    }
    let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    assert!(N1NativeClient::connect(&server.native).await.is_err());
    let mut client = N1NativeClient::connect_with_tls(&server.native, Some(Arc::new(tls)))
        .await
        .unwrap();
    client.auth_fragment("test", SECRET).await.unwrap();
    assert_eq!(
        client.control(Command::Ping).await.unwrap().result.unwrap()["noa"],
        2
    );
    assert!(server.fixture.base.is_dir());
}

#[tokio::test]
async fn normal_and_batch_uploads_share_native_and_http_noa_handlers() {
    let server = Server::new(false).await;
    let mut client = server.client().await;
    let data = vec![23; 9000];
    let first = init(&mut client, data.len() as u64).await;
    client
        .put(
            string(&first, "object_key"),
            data.len() as u64,
            &mut std::io::Cursor::new(data.clone()),
        )
        .await
        .unwrap();
    let mut keys = Vec::new();
    let mut body = Vec::new();
    for _ in 0..3 {
        let item = init(&mut client, data.len() as u64).await;
        keys.push(string(&item, "object_key"));
        body.extend_from_slice(&data);
    }
    let completed = client
        .put_batch(
            keys.clone(),
            body.len() as u64,
            &mut std::io::Cursor::new(body),
        )
        .await
        .unwrap();
    assert_eq!(
        completed.result.unwrap()["objects"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    let login = client.auth_fragment("test", SECRET).await.unwrap();
    let token = string(&login.result.unwrap(), "token");
    let item = init(&mut client, data.len() as u64).await;
    let request = Request {
        id: 12,
        command: Command::UploadPut {
            object_key: string(&item, "object_key"),
        },
        body_len: data.len() as u64,
    };
    let control = rmp_serde::to_vec_named(&request).unwrap();
    let mut bytes = (control.len() as u32).to_be_bytes().to_vec();
    bytes.extend(control);
    bytes.extend(&data);
    let response = reqwest::Client::new()
        .post(format!("{}/noa/stream", server.http))
        .bearer_auth(token)
        .body(bytes)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.json::<Value>().await.unwrap()["result"]["object_key"],
        item["object_key"]
    );
    keys.push(string(&item, "object_key"));
    keys.push(string(&first, "object_key"));
    for key in keys {
        let mut output = Vec::new();
        client.download(key, 0, None, &mut output).await.unwrap();
        assert_eq!(output, data);
    }
}

#[tokio::test]
async fn stopping_optional_scrub_keeps_native_io_available_and_scoped_tokens_cannot_start_jobs() {
    let server = Server::new(false).await;
    let mut client = server.client().await;
    let source = vec![19; 8000];
    let item = init(&mut client, source.len() as u64).await;
    let key = string(&item, "object_key");
    client
        .put(
            key.clone(),
            source.len() as u64,
            &mut std::io::Cursor::new(source.clone()),
        )
        .await
        .unwrap();
    assert!(
        client
            .control(Command::MaintenanceJob)
            .await
            .unwrap()
            .result
            .is_none_or(|v| v.is_null())
    );
    let job = client
        .control(Command::Scrub {
            options: n1_link::noa::ScrubOptions {
                repair: true,
                bytes_per_second: 1,
                window_seconds: 60,
            },
        })
        .await
        .unwrap()
        .result
        .unwrap();
    let id = string(&job, "id");
    let fp = server
        .service
        .store
        .inspect_upload([11; 16], &key)
        .await
        .unwrap()
        .identity
        .fingerprint();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !server.service.store.background_active(fp) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    client
        .control(Command::MaintenanceStop { job_id: id })
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let status = client
                .control(Command::MaintenanceJob)
                .await
                .unwrap()
                .result
                .unwrap();
            if !matches!(status["phase"].as_str(), Some("running" | "stopping")) {
                assert_eq!(status["phase"], "stopped");
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        client.control(Command::Ping).await.unwrap().result.unwrap()["noa"],
        2
    );
    let mut bytes = Vec::new();
    client
        .download(string(&item, "object_key"), 0, None, &mut bytes)
        .await
        .unwrap();
    assert_eq!(bytes, source);
    client
        .control(Command::Login {
            fragment: "test".into(),
            secret: SECRET.into(),
            permissions: Some(Permissions {
                read: true,
                ..Permissions::default()
            }),
            ttl_seconds: Some(60),
        })
        .await
        .unwrap();
    is_status(client.control(Command::GarbageCollect).await, "403");
}

#[tokio::test]
async fn schedule_is_opt_in_and_starts_only_the_configured_job() {
    let server = Server::new(false).await;
    let stop = tokio_util::sync::CancellationToken::new();
    let jobs = &server.service.jobs;
    jobs.schedule(
        server.service.store.clone(),
        crate::core::maintenance::jobs::MaintenanceConfig::default(),
        stop.clone(),
    );
    assert!(jobs.status().unwrap().is_none());
    let configured = crate::core::maintenance::jobs::MaintenanceConfig {
        gc: Some(crate::core::maintenance::jobs::Schedule {
            every_seconds: 60,
            start_delay_seconds: 0,
        }),
        scrub: None,
        calendar_scrub: None,
    };
    configured.validate().unwrap();
    jobs.schedule(server.service.store.clone(), configured, stop.clone());
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Some(status) = jobs.status().unwrap()
                && status.phase == "completed"
            {
                assert_eq!(status.kind, "gc");
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    stop.cancel();
}

#[tokio::test]
async fn authenticated_native_replication_works_with_http_stopped_and_denies_scoped_tokens() {
    let server = Server::new_recovery(false, true).await;
    server.handles[1].abort();
    let repository = &server.service.config.repositories["archive"];
    let mut denied = server.client().await;
    denied
        .control(Command::Login {
            fragment: "test".into(),
            secret: SECRET.into(),
            permissions: Some(Permissions {
                read: true,
                ..Permissions::default()
            }),
            ttl_seconds: Some(60),
        })
        .await
        .unwrap();
    assert!(
        matches!(denied.into_replication("archive".into()).await,Err(NativeClientError::Native{code,..})if code=="403")
    );
    assert!(!repository.path.join(".lock").exists());
    let mut client = server.client().await;
    let object = init(&mut client, 20000).await;
    client
        .put(
            string(&object, "object_key"),
            20000,
            &mut std::io::Cursor::new(vec![15; 20000]),
        )
        .await
        .unwrap();
    let source = server.fixture.base.join("source-repository");
    crate::core::backup::repository::Repository::initialize(&source, &repository.key_file).unwrap();
    let point = server
        .service
        .store
        .snapshot_online(source.clone(), repository.key_file.clone(), 30)
        .await
        .unwrap();
    let source = Arc::new(
        crate::core::backup::repository::Repository::open(&source, &repository.key_file, false)
            .unwrap(),
    );
    let client = server.client().await;
    let mut channel = client.into_replication("archive".into()).await.unwrap();
    assert_eq!(
        crate::core::backup::network::send(&mut channel, source, point.id.clone())
            .await
            .unwrap()
            .id,
        point.id
    );
    drop(channel);
    let destination = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match server.service.open_repository("archive", false).await {
                Ok(repo) => break repo,
                Err(crate::core::Error::Unavailable(_)) => tokio::task::yield_now().await,
                Err(e) => panic!("{e}"),
            }
        }
    })
    .await
    .unwrap();
    destination
        .verify(&destination.snapshot(&point.id).unwrap())
        .unwrap();
}

async fn recovery_done(client: &mut N1NativeClient, id: &str) -> Value {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let status = client
                .control(Command::BackupJob)
                .await
                .unwrap()
                .result
                .unwrap();
            assert_eq!(status["id"], id);
            if status["phase"] != "running" && status["phase"] != "stopping" {
                return status;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn recovery_control_jobs_share_native_http_authorization_and_report_real_verification_failures()
 {
    let server = Server::new_recovery(false, true).await;
    let mut client = server.client().await;
    let object = init(&mut client, 20000).await;
    client
        .put(
            string(&object, "object_key"),
            20000,
            &mut std::io::Cursor::new(vec![57; 20000]),
        )
        .await
        .unwrap();
    let capture = client
        .control(Command::BackupCapture {
            repository: "archive".into(),
            boundary_seconds: 30,
        })
        .await
        .unwrap()
        .result
        .unwrap();
    let captured = recovery_done(&mut client, &string(&capture, "id")).await;
    assert_eq!(captured["phase"], "completed", "{captured}");
    let snapshot = string(&captured["result"], "id");
    let listed = client
        .control(Command::BackupList {
            repository: "archive".into(),
            after: String::new(),
            limit: 1,
        })
        .await
        .unwrap()
        .result
        .unwrap();
    assert_eq!(listed.as_array().unwrap().len(), 1);
    assert_eq!(listed[0]["id"], snapshot);
    is_status(
        client
            .control(Command::BackupList {
                repository: "archive".into(),
                after: String::new(),
                limit: 257,
            })
            .await,
        "400",
    );
    is_status(
        client
            .control(Command::BackupList {
                repository: "archive".into(),
                after: "../invalid".into(),
                limit: 1,
            })
            .await,
        "400",
    );
    is_status(
        client
            .control(Command::BackupVerify {
                repository: "archive".into(),
                snapshot: "../invalid".into(),
            })
            .await,
        "400",
    );
    let login = server
        .service
        .auth
        .login("test".into(), SECRET.into(), None, None)
        .await
        .unwrap();
    let response: n1_link::noa::Response = reqwest::Client::new()
        .post(format!("{}/noa", server.http))
        .bearer_auth(&login.token)
        .json(&Request {
            id: 9,
            body_len: 0,
            command: Command::BackupVerify {
                repository: "archive".into(),
                snapshot: snapshot.clone(),
            },
        })
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let verified = recovery_done(&mut client, &string(&response.result.unwrap(), "id")).await;
    assert_eq!(verified["phase"], "completed", "{verified}");
    let mut scoped = server.client().await;
    scoped
        .control(Command::Login {
            fragment: "test".into(),
            secret: SECRET.into(),
            permissions: Some(Permissions {
                read: true,
                ..Permissions::default()
            }),
            ttl_seconds: Some(60),
        })
        .await
        .unwrap();
    is_status(
        scoped
            .control(Command::BackupCapture {
                repository: "archive".into(),
                boundary_seconds: 30,
            })
            .await,
        "403",
    );
    is_status(scoped.control(Command::BackupJob).await, "403");
    let configured = &server.service.config.repositories["archive"];
    let repo = crate::core::backup::repository::Repository::open(
        &configured.path,
        &configured.key_file,
        true,
    )
    .unwrap();
    let point = repo.snapshot(&snapshot).unwrap();
    let hash = &point.manifest[0];
    let path = repo.path.join("blocks").join(&hash[..2]).join(hash);
    let mut damaged = std::fs::read(&path).unwrap();
    damaged[0] ^= 1;
    std::fs::write(path, damaged).unwrap();
    drop(repo);
    let started = client
        .control(Command::BackupVerify {
            repository: "archive".into(),
            snapshot,
        })
        .await
        .unwrap()
        .result
        .unwrap();
    let failed = recovery_done(&mut client, &string(&started, "id")).await;
    assert_eq!(failed["phase"], "incomplete");
    assert!(failed["error"].is_string());
    client.control(Command::Ping).await.unwrap();
}

#[tokio::test]
async fn http_replication_uses_the_same_authenticated_recovery_protocol() {
    let server = Server::new_recovery(false, true).await;
    let repository = &server.service.config.repositories["archive"];
    let mut client = server.client().await;
    let object = init(&mut client, 15000).await;
    client
        .put(
            string(&object, "object_key"),
            15000,
            &mut std::io::Cursor::new(vec![29; 15000]),
        )
        .await
        .unwrap();
    let source = server.fixture.base.join("http-source");
    crate::core::backup::repository::Repository::initialize(&source, &repository.key_file).unwrap();
    let point = server
        .service
        .store
        .snapshot_online(source.clone(), repository.key_file.clone(), 30)
        .await
        .unwrap();
    let source = Arc::new(
        crate::core::backup::repository::Repository::open(&source, &repository.key_file, false)
            .unwrap(),
    );
    let credentials = cli_core::Credentials {
        native_url: None,
        fragment: Some("test".into()),
        secret: Some(SECRET.into()),
        token: None,
    };
    assert_eq!(
        crate::core::backup::network::replicate_http(
            &server.http,
            "archive",
            credentials,
            None,
            source,
            point.id.clone()
        )
        .await
        .unwrap()
        .id,
        point.id
    );
    let destination = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match server.service.open_repository("archive", false).await {
                Ok(repo) => break repo,
                Err(crate::core::Error::Unavailable(_)) => tokio::task::yield_now().await,
                Err(e) => panic!("{e}"),
            }
        }
    })
    .await
    .unwrap();
    destination
        .verify(&destination.snapshot(&point.id).unwrap())
        .unwrap();
}
