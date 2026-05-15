use clap::Parser;
use futures_util::future::join_all;
use futures_util::{SinkExt, StreamExt};
use native_tls::TlsConnector;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::header::CONTENT_LENGTH;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{
    Connector, MaybeTlsStream, WebSocketStream, connect_async_tls_with_config,
};
use tokio_util::io::ReaderStream;

type ProofResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DEFAULT_MAX_PART_SIZE: u64 = 52_428_800;

#[derive(Parser, Debug)]
#[command(about = "Rust-only N1 release proof matrix runner")]
struct Args {
    #[arg(long)]
    binary: PathBuf,
    #[arg(long)]
    source_file: PathBuf,
    #[arg(long)]
    disk_roots: String,
    #[arg(long)]
    out_dir: PathBuf,
    #[arg(long, default_value_t = 50_671)]
    port: u16,
    #[arg(long, default_value = "release-proof/")]
    prefix: String,
    #[arg(long, default_value_t = 2_147_483_648)]
    quota_bytes: u64,
    #[arg(long, default_value = "disabled")]
    quota_backend: String,
    #[arg(long, default_value_t = DEFAULT_MAX_PART_SIZE)]
    max_part_size: u64,
    #[arg(long, default_value = "n1-release-proof-master")]
    master_key: String,
    #[arg(long, default_value = "n1-release-proof-fragment")]
    fragment_key: String,
    #[arg(long, default_value = "")]
    fragment_name: String,
    #[arg(long, default_value_t = false)]
    cleanup_roots: bool,
    #[arg(long, default_value_t = 10_800)]
    job_timeout_seconds: u64,
    #[arg(long, default_value_t = 2)]
    large_copies: usize,
    #[arg(long, default_value_t = 10_000)]
    storm_objects: usize,
    #[arg(long, default_value_t = 24)]
    storm_concurrency: usize,
    #[arg(long, default_value_t = 16_384)]
    storm_object_size: usize,
    #[arg(long, default_value_t = 4)]
    mixed_users: usize,
    #[arg(long, default_value_t = 1_000)]
    mixed_files_per_user: usize,
    #[arg(long, default_value_t = 1_000)]
    mixed_background_uploads: usize,
    #[arg(long, default_value_t = 4)]
    multi_fragment_users: usize,
    #[arg(long, default_value_t = 250)]
    multi_fragment_files_per_user: usize,
    #[arg(long, default_value_t = false)]
    chaos: bool,
    #[arg(long, default_value_t = 0)]
    chaos_seconds: u64,
    #[arg(long, default_value_t = 0)]
    chaos_cycles: usize,
    #[arg(long, default_value_t = 1_000)]
    chaos_storm_objects: usize,
}

#[derive(Debug)]
struct Proof {
    args: Args,
    stamp: String,
    client: Client,
    base_url: String,
    index_roots: Vec<PathBuf>,
    blob_roots: Vec<PathBuf>,
    server: Option<Child>,
    fragment: String,
    token: String,
    summary: Value,
    active_disk_count: usize,
    parity_shards: u16,
}

struct ActorReport {
    final_keys: Vec<String>,
    report: Value,
}

#[derive(Deserialize)]
struct FragmentCreate {
    fragment_hash: String,
}

#[derive(Deserialize)]
struct AuthResponse {
    access_token: String,
}

#[derive(Deserialize)]
struct MultipartInit {
    version_id: String,
    session_id: String,
}

impl Proof {
    fn new(args: Args) -> ProofResult<Self> {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs()
            .to_string();
        let request_timeout = Duration::from_secs(args.job_timeout_seconds.max(900) + 600);
        let client = Client::builder()
            .danger_accept_invalid_certs(true)
            .timeout(request_timeout)
            .build()?;
        let base_url = format!("https://127.0.0.1:{}", args.port);
        Ok(Self {
            args,
            stamp,
            client,
            base_url,
            index_roots: Vec::new(),
            blob_roots: Vec::new(),
            server: None,
            fragment: String::new(),
            token: String::new(),
            summary: json!({}),
            active_disk_count: 0,
            parity_shards: 1,
        })
    }

    async fn run(&mut self) -> ProofResult<()> {
        self.validate()?;
        self.prepare_dirs()?;
        self.write_config()?;
        self.start_server("before").await?;
        self.create_fragment().await?;
        self.auth().await?;

        let object_key = self.object_key();
        let source_len = self.args.source_file.metadata()?.len();
        if source_len <= self.args.max_part_size {
            return Err(format!(
                "source file is too small for large multipart proof: {} <= max_part_size {}",
                source_len, self.args.max_part_size
            )
            .into());
        }

        let mut live_expected = BTreeSet::new();
        let mut evidence = Vec::new();
        let mut rss_samples = Vec::new();
        let local_sha = self.sha256_file(&self.args.source_file).await?;
        rss_samples.push(self.rss_sample("server_started"));
        let upload = self.multipart_restart_resume(&object_key).await?;
        live_expected.insert(object_key.clone());
        self.assert_list_contains(&object_key).await?;
        let downloaded_sha = self.download_sha256(&object_key).await?;
        if downloaded_sha != local_sha {
            return Err(format!(
                "download hash mismatch for {object_key}: local={local_sha} remote={downloaded_sha}"
            )
            .into());
        }
        self.range_probe(&object_key).await?;
        self.assert_object_status_clean(&object_key, "after_large_multipart")
            .await?;
        self.assert_health_clean("after_large_multipart").await?;
        rss_samples.push(self.rss_sample("after_large_multipart"));
        evidence.push(json!({"gate": "large_multipart_restart_resume", "status": "PASS"}));

        self.stop_server()?;
        self.start_server("after_restart").await?;
        self.auth().await?;
        self.assert_expected_visible(&live_expected).await?;
        self.assert_download_sha256(&object_key, &local_sha).await?;
        rss_samples.push(self.rss_sample("after_restart_persistence"));
        evidence.push(json!({"gate": "restart_persistence", "status": "PASS"}));

        let permissions = self.permission_boundary_proof(&mut live_expected).await?;
        evidence.push(json!({"gate": "auth_permission_boundaries", "status": "PASS"}));

        let quota = self.quota_api_proof().await?;
        evidence.push(json!({"gate": "quota_api_and_usage", "status": "PASS"}));

        let rename_lineage = self
            .rename_version_lineage_proof(&mut live_expected)
            .await?;
        rss_samples.push(self.rss_sample("after_rename_version_lineage"));
        evidence.push(json!({"gate": "rename_preserves_version_lineage", "status": "PASS"}));

        let websocket = self
            .websocket_namespace_delta_proof(&mut live_expected)
            .await?;
        rss_samples.push(self.rss_sample("after_websocket_namespace_delta"));
        evidence.push(json!({"gate": "websocket_namespace_delta", "status": "PASS"}));

        let additional_large = self
            .multiple_large_multipart_proof(&mut live_expected, &local_sha)
            .await?;
        rss_samples.push(self.rss_sample("after_multiple_large_multipart"));
        evidence.push(json!({"gate": "multiple_large_multipart", "status": "PASS"}));

        let object_storm = self
            .concurrent_object_storm(&mut live_expected, "main-storm", self.args.storm_objects)
            .await?;
        rss_samples.push(self.rss_sample("after_10k_concurrent_object_storm"));
        evidence.push(json!({"gate": "large_object_count_concurrency", "status": "PASS"}));

        let same_fragment_mixed = self
            .same_fragment_mixed_user_proof(&mut live_expected)
            .await?;
        rss_samples.push(self.rss_sample("after_same_fragment_mixed_users"));
        evidence.push(json!({"gate": "same_fragment_mixed_users", "status": "PASS"}));

        let parallel_multipart = self
            .parallel_multipart_user_proof(&mut live_expected)
            .await?;
        rss_samples.push(self.rss_sample("after_parallel_multipart_users"));
        evidence.push(json!({"gate": "parallel_multipart_users", "status": "PASS"}));

        let version_contention = self
            .same_key_version_contention_proof(&mut live_expected)
            .await?;
        rss_samples.push(self.rss_sample("after_same_key_version_contention"));
        evidence.push(json!({"gate": "same_key_version_contention", "status": "PASS"}));

        let multi_fragment = self.multi_fragment_isolation_proof().await?;
        rss_samples.push(self.rss_sample("after_multi_fragment_users"));
        evidence.push(json!({"gate": "multi_fragment_user_isolation", "status": "PASS"}));

        let crud = self.posix_crud_gc_proof(&mut live_expected).await?;
        rss_samples.push(self.rss_sample("after_posix_crud_gc"));
        evidence.push(json!({"gate": "posix_crud_trash_recover_gc", "status": "PASS"}));

        let small_multipart = self.small_multipart_proof(&mut live_expected).await?;
        rss_samples.push(self.rss_sample("after_small_multipart"));
        evidence.push(json!({"gate": "small_multipart", "status": "PASS"}));

        let archive = self.archive_proof(&mut live_expected).await?;
        evidence.push(json!({"gate": "archive_tar", "status": "PASS"}));

        let recover_on_read = self
            .simulate_recover_on_read(&object_key, &local_sha)
            .await?;
        rss_samples.push(self.rss_sample("after_recover_on_read"));
        evidence.push(json!({"gate": "recover_on_read", "status": "PASS"}));

        let object_repair = self.object_repair_status_proof(&object_key).await?;
        rss_samples.push(self.rss_sample("after_object_repair"));
        evidence.push(json!({"gate": "object_status_repair", "status": "PASS"}));

        let disk_repair = self
            .simulate_disk_format_and_repair(&live_expected, &object_key, &local_sha)
            .await?;
        rss_samples.push(self.rss_sample("after_disk_format_repair"));
        evidence.push(json!({"gate": "disk_format_repair", "status": "PASS"}));

        let redistribution = self.redistribution_proof(&mut live_expected).await?;
        rss_samples.push(self.rss_sample("after_redistribution"));
        evidence.push(json!({"gate": "redistribution_2_1_2", "status": "PASS"}));

        let chaos = if self.args.chaos || self.args.chaos_seconds > 0 || self.args.chaos_cycles > 0
        {
            let report = self
                .chaos_proof(&mut live_expected, &local_sha, &mut rss_samples)
                .await?;
            evidence.push(json!({"gate": "chaos", "status": "PASS"}));
            report
        } else {
            json!({"enabled": false})
        };

        self.assert_expected_visible(&live_expected).await?;
        self.assert_health_clean("final").await?;
        rss_samples.push(self.rss_sample("final_health_clean"));
        let inspection = self.inspect_index(live_expected.len())?;
        self.stop_server()?;
        let logs = self.scan_logs()?;

        self.summary = json!({
            "status": "PASS",
            "engine": "n1",
            "proof": "n1_release_matrix",
            "source_file": self.args.source_file,
            "object_key": object_key,
            "source_bytes": source_len,
            "source_sha256": local_sha,
            "download_sha256": downloaded_sha,
            "max_part_size": self.args.max_part_size,
            "upload": upload,
            "additional_large_multipart": additional_large,
            "object_storm": object_storm,
            "same_fragment_mixed_users": same_fragment_mixed,
            "parallel_multipart_users": parallel_multipart,
            "same_key_version_contention": version_contention,
            "multi_fragment_user_isolation": multi_fragment,
            "permissions": permissions,
            "quota": quota,
            "rename_version_lineage": rename_lineage,
            "websocket": websocket,
            "crud": crud,
            "small_multipart": small_multipart,
            "archive": archive,
            "recover_on_read": recover_on_read,
            "object_repair": object_repair,
            "disk_format_repair": disk_repair,
            "redistribution": redistribution,
            "chaos": chaos,
            "live_object_count": live_expected.len(),
            "evidence": evidence,
            "rss_samples": rss_samples,
            "log_scan": logs,
            "index_inspection": inspection,
            "fragment_hash": self.fragment,
            "index_roots": self.index_roots,
            "blob_roots": self.blob_roots,
        });
        self.write_summary()?;
        if self.args.cleanup_roots {
            self.cleanup_roots()?;
        }
        Ok(())
    }

    fn validate(&mut self) -> ProofResult<()> {
        if !self.args.binary.is_file() {
            return Err(format!("N1 binary does not exist: {}", self.args.binary.display()).into());
        }
        if !self.args.source_file.is_file() {
            return Err(format!(
                "source file does not exist: {}",
                self.args.source_file.display()
            )
            .into());
        }
        if !self.args.prefix.ends_with('/') {
            self.args.prefix.push('/');
        }
        let roots = self.disk_roots()?;
        self.active_disk_count = roots.len();
        if roots.len() < 2 {
            return Err("release proof requires at least two disk roots".into());
        }
        if self.args.large_copies == 0 {
            return Err("large_copies must be at least 1".into());
        }
        if self.args.storm_objects == 0 {
            return Err("storm_objects must be at least 1".into());
        }
        if self.args.storm_concurrency == 0 {
            return Err("storm_concurrency must be at least 1".into());
        }
        if self.args.storm_object_size == 0 {
            return Err("storm_object_size must be at least 1".into());
        }
        if self.args.mixed_users == 0 {
            return Err("mixed_users must be at least 1".into());
        }
        if self.args.mixed_files_per_user == 0 {
            return Err("mixed_files_per_user must be at least 1".into());
        }
        if self.args.multi_fragment_users == 0 {
            return Err("multi_fragment_users must be at least 1".into());
        }
        if self.args.multi_fragment_files_per_user == 0 {
            return Err("multi_fragment_files_per_user must be at least 1".into());
        }
        for root in &roots {
            if !root.is_dir() {
                return Err(format!("disk root does not exist: {}", root.display()).into());
            }
        }
        Ok(())
    }

    fn disk_roots(&self) -> ProofResult<Vec<PathBuf>> {
        let roots = self
            .args
            .disk_roots
            .split(',')
            .filter(|item| !item.trim().is_empty())
            .map(|item| PathBuf::from(item.trim()))
            .collect::<Vec<_>>();
        Ok(roots)
    }

    fn prepare_dirs(&mut self) -> ProofResult<()> {
        fs::create_dir_all(&self.args.out_dir)?;
        fs::create_dir_all(self.args.out_dir.join("fs"))?;
        fs::create_dir_all(self.args.out_dir.join("refresh"))?;
        let proof_root = format!("n1-release-proof-{}", self.stamp);
        for disk in self.disk_roots()? {
            let root = disk.join(&proof_root);
            let index = root.join("index");
            let blob = root.join("blob");
            fs::create_dir_all(&index)?;
            fs::create_dir_all(&blob)?;
            self.index_roots.push(index);
            self.blob_roots.push(blob);
        }
        Ok(())
    }

    fn write_config(&self) -> ProofResult<()> {
        let cert = self.args.out_dir.join("cert.pem");
        let key = self.args.out_dir.join("key.pem");
        if !cert.exists() || !key.exists() {
            let status = Command::new("openssl")
                .args([
                    "req", "-x509", "-newkey", "rsa:2048", "-sha256", "-days", "1", "-nodes",
                    "-keyout",
                ])
                .arg(&key)
                .arg("-out")
                .arg(&cert)
                .args(["-subj", "/CN=127.0.0.1"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()?;
            if !status.success() {
                return Err("openssl failed to create local TLS certificate".into());
            }
        }
        fs::write(
            self.args.out_dir.join("n1.env"),
            format!(
                "JWT_SECRET=n1-release-proof-jwt-secret\nMASTER_KEY={}\nAUTH_KEY={}\nADMIN_KEY={}\nACCESS_TOKEN_DEFAULT_TTL_SECS=3600\nACCESS_TOKEN_MAX_TTL_SECS=21600\nREFRESH_TOKEN_TTL_SECS=604800\n",
                self.args.master_key, self.args.master_key, self.args.master_key
            ),
        )?;
        let index_roots = self
            .index_roots
            .iter()
            .take(self.active_disk_count)
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(",");
        let blob_roots = self
            .blob_roots
            .iter()
            .take(self.active_disk_count)
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(",");
        let config = format!(
            r#"[server]
host=127.0.0.1
https_port={}
cert_path={}
key_path={}

[disk_config]
index_store_disks={}
blob_store_disks={}
fs_storage_path={}

[quota_config]
fragment_quota_backend={}

[index_config]
partition_count=64
per_db_cache_bytes=auto
max_cached_dbs=auto

[namespace_config]
max_loaded_facades=auto
max_loaded_dbs=auto

[garbage_collector]
cleanup_interval_seconds=86400
file_ttl_seconds=30
retention_seconds=2592000

[set_limits]
max_file_size=1099511627776

[jwt_token_config]
login_token_expiration=900

[rate_limiter_config]
enable_rate_limiting=false
auth_limit_per_second=5000
auth_limit_burst_size=10000
view_limit_per_second=5000
view_limit_burst_size=10000
upload_limit_per_second=5000
upload_limit_burst_size=10000
global_limit_per_second=5000
global_limit_burst_size=10000

[upload_config]
max_concurrent_io=auto
adaptive_concurrency_enabled=true
adaptive_concurrency_interval_ms=auto
writer_buffer_size=auto
upload_memory_budget_bytes=auto
upload_part_strict_durability_default=false
upload_io_gate_enabled=false
stream_max_concurrency=auto
stream_chunk_size=auto
stream_readahead_bytes=auto
stream_locality_bias_enabled=true
monitor_interval_seconds=auto
monitor_deep_scan_interval_seconds=auto
max_part_size={}
data_shards=1
parity_shards={}
stripe_block_size=262144
"#,
            self.args.port,
            cert.display(),
            key.display(),
            index_roots,
            blob_roots,
            self.args.out_dir.join("fs").display(),
            self.args.quota_backend,
            self.args.max_part_size,
            self.parity_shards
        );
        fs::write(self.args.out_dir.join("n1_config.ini"), config)?;
        Ok(())
    }

    async fn start_server(&mut self, label: &str) -> ProofResult<()> {
        let log = fs::File::create(self.args.out_dir.join(format!("server-{label}.log")))?;
        let err = log.try_clone()?;
        let child = Command::new(&self.args.binary)
            .current_dir(&self.args.out_dir)
            .env("RUST_LOG", "info")
            .env("RUST_BACKTRACE", "1")
            .env("N1_REFRESH_STORE_ROOT", self.args.out_dir.join("refresh"))
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(err))
            .spawn()?;
        self.server = Some(child);
        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            if let Some(child) = self.server.as_mut() {
                if let Some(status) = child.try_wait()? {
                    return Err(format!("N1 exited during startup with {status}").into());
                }
            }
            if let Ok(response) = self
                .client
                .get(format!("{}/n1/ping", self.base_url))
                .send()
                .await
            {
                if response.status() == StatusCode::OK
                    && response.bytes().await?.as_ref() == b"pong"
                {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Err("N1 did not become ready".into())
    }

    fn stop_server(&mut self) -> ProofResult<()> {
        if let Some(mut child) = self.server.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        Ok(())
    }

    async fn create_fragment(&mut self) -> ProofResult<()> {
        let name = if self.args.fragment_name.is_empty() {
            format!("release-proof-{}", self.stamp)
        } else {
            self.args.fragment_name.clone()
        };
        let body = json!({
            "name": name,
            "secret": self.args.master_key,
            "key": self.args.fragment_key,
            "quota_bytes": self.args.quota_bytes,
        });
        let response = self
            .client
            .post(format!("{}/n1/fragments", self.base_url))
            .json(&body)
            .send()
            .await?;
        if response.status() != StatusCode::CREATED {
            return Err(format!(
                "fragment create failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let created: FragmentCreate = response.json().await?;
        self.fragment = created.fragment_hash;
        Ok(())
    }

    async fn auth(&mut self) -> ProofResult<()> {
        let body = json!({
            "fragment": self.fragment,
            "secret": self.args.fragment_key,
            "read": true,
            "write": true,
            "modify": true,
            "delete": true,
            "expires_in": 21600,
        });
        let response = self
            .client
            .post(format!("{}/n1/auth/fragment", self.base_url))
            .json(&body)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "fragment auth failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let auth: AuthResponse = response.json().await?;
        self.token = auth.access_token;
        Ok(())
    }

    async fn auth_fragment_permissions(
        &self,
        fragment: &str,
        key: &str,
        read: bool,
        write: bool,
        modify: bool,
        delete: bool,
    ) -> ProofResult<String> {
        let response = self
            .client
            .post(format!("{}/n1/auth/fragment", self.base_url))
            .json(&json!({
                "fragment": fragment,
                "secret": key,
                "read": read,
                "write": write,
                "modify": modify,
                "delete": delete,
                "expires_in": 21600,
            }))
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "fragment auth with permissions failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(response.json::<AuthResponse>().await?.access_token)
    }

    async fn permission_boundary_proof(
        &self,
        expected: &mut BTreeSet<String>,
    ) -> ProofResult<Value> {
        let object_key = format!("{}auth-boundary/readable.txt", self.args.prefix);
        let payload = b"n1 permission boundary\n".to_vec();
        let sha = sha256_bytes(&payload);
        self.upload_bytes(&object_key, payload, "text/plain")
            .await?;
        expected.insert(object_key.clone());

        let read_only = self
            .auth_fragment_permissions(
                &self.fragment,
                &self.args.fragment_key,
                true,
                false,
                false,
                false,
            )
            .await?;
        self.assert_list_contains_with_token(&read_only, &object_key)
            .await?;
        self.assert_download_sha256_with_token(&read_only, &object_key, &sha)
            .await?;

        let denied_upload = self
            .client
            .post(format!(
                "{}/n1/upload/one-shot/{}",
                self.base_url,
                encode_path(&format!(
                    "{}auth-boundary/should-deny.txt",
                    self.args.prefix
                ))
            ))
            .bearer_auth(&read_only)
            .header("content-type", "text/plain")
            .body("must not write")
            .send()
            .await?;
        if denied_upload.status() != StatusCode::FORBIDDEN {
            return Err(format!(
                "read-only token was allowed to upload or returned wrong status: {} {}",
                denied_upload.status(),
                denied_upload.text().await?
            )
            .into());
        }

        let denied_mkdir = self
            .client
            .post(format!("{}/n1/posix/mkdir", self.base_url))
            .bearer_auth(&read_only)
            .json(&json!({"path": format!("{}auth-boundary/should-deny-dir", self.args.prefix)}))
            .send()
            .await?;
        if denied_mkdir.status() != StatusCode::FORBIDDEN {
            return Err(format!(
                "read-only token was allowed to mkdir or returned wrong status: {} {}",
                denied_mkdir.status(),
                denied_mkdir.text().await?
            )
            .into());
        }

        Ok(json!({
            "read_only_list_download": "PASS",
            "write_denied_status": StatusCode::FORBIDDEN.as_u16(),
            "mkdir_denied_status": StatusCode::FORBIDDEN.as_u16(),
            "sample_object": object_key,
        }))
    }

    async fn quota_api_proof(&self) -> ProofResult<Value> {
        let usage = self
            .client
            .get(format!("{}/n1/capacity/quota", self.base_url))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if usage.status() != StatusCode::OK {
            return Err(format!(
                "quota usage failed: {} {}",
                usage.status(),
                usage.text().await?
            )
            .into());
        }
        let usage_body: Value = usage.json().await?;
        let quota_bytes = usage_body
            .get("quota_bytes")
            .and_then(Value::as_u64)
            .ok_or_else(|| format!("quota usage missing quota_bytes: {usage_body}"))?;
        if quota_bytes != self.args.quota_bytes {
            return Err(format!(
                "quota usage reported wrong quota_bytes: expected={} actual={} body={}",
                self.args.quota_bytes, quota_bytes, usage_body
            )
            .into());
        }
        let used_bytes = usage_body
            .get("used_bytes")
            .and_then(Value::as_u64)
            .ok_or_else(|| format!("quota usage missing used_bytes: {usage_body}"))?;
        if used_bytes == 0 {
            return Err(
                format!("quota usage stayed at zero after large upload: {usage_body}").into(),
            );
        }

        let too_small = self
            .client
            .patch(format!(
                "{}/n1/fragments/{}/quota",
                self.base_url, self.fragment
            ))
            .bearer_auth(&self.token)
            .json(&json!({"secret": self.args.master_key, "quota_bytes": 1_u64}))
            .send()
            .await?;
        if too_small.status() != StatusCode::CONFLICT {
            return Err(format!(
                "quota shrink below used bytes returned wrong status: {} {}",
                too_small.status(),
                too_small.text().await?
            )
            .into());
        }

        let unchanged = self
            .client
            .patch(format!(
                "{}/n1/fragments/{}/quota",
                self.base_url, self.fragment
            ))
            .bearer_auth(&self.token)
            .json(&json!({"secret": self.args.master_key, "quota_bytes": self.args.quota_bytes}))
            .send()
            .await?;
        if unchanged.status() != StatusCode::OK {
            return Err(format!(
                "quota unchanged resize failed: {} {}",
                unchanged.status(),
                unchanged.text().await?
            )
            .into());
        }
        let unchanged_body: Value = unchanged.json().await?;

        Ok(json!({
            "usage": usage_body,
            "shrink_below_used_status": StatusCode::CONFLICT.as_u16(),
            "unchanged_resize": unchanged_body,
        }))
    }

    async fn rename_version_lineage_proof(
        &self,
        expected: &mut BTreeSet<String>,
    ) -> ProofResult<Value> {
        let old_key = format!("{}rename-lineage/original-name.bin", self.args.prefix);
        let new_key = format!("{}rename-lineage/renamed-name.bin", self.args.prefix);
        let first_payload = deterministic_payload(256 * 1024, b"rename-lineage-v1");
        let second_payload = deterministic_payload(384 * 1024, b"rename-lineage-v2");
        let first_sha = sha256_bytes(&first_payload);
        let second_sha = sha256_bytes(&second_payload);

        let first_version = self
            .upload_bytes(&old_key, first_payload, "application/octet-stream")
            .await?;
        self.assert_download_sha256(&old_key, &first_sha).await?;

        self.posix_post(
            "/n1/posix/rename",
            json!({"source_path": &old_key, "destination_path": &new_key}),
        )
        .await?;
        self.assert_list_contains(&new_key).await?;
        self.assert_download_sha256(&new_key, &first_sha).await?;
        self.expect_download_status(&old_key, StatusCode::NOT_FOUND)
            .await?;

        let second_version = self
            .upload_bytes(&new_key, second_payload, "application/octet-stream")
            .await?;
        self.assert_download_sha256(&new_key, &second_sha).await?;

        let versions = self.list_version_ids(&new_key).await?;
        if !versions.contains(&first_version) || !versions.contains(&second_version) {
            return Err(format!(
                "rename lineage lost version history for {new_key}: first={first_version} second={second_version} versions={versions:?}"
            )
            .into());
        }
        if versions.len() < 2 {
            return Err(format!(
                "rename lineage expected at least two versions for {new_key}: {versions:?}"
            )
            .into());
        }

        expected.insert(new_key.clone());
        Ok(json!({
            "old_key": old_key,
            "new_key": new_key,
            "first_version": first_version,
            "second_version": second_version,
            "version_count": versions.len(),
        }))
    }

    async fn websocket_namespace_delta_proof(
        &self,
        expected: &mut BTreeSet<String>,
    ) -> ProofResult<Value> {
        let token = encode_query_value(&self.token);
        let ws_url = format!("wss://127.0.0.1:{}/ws?token={}", self.args.port, token);
        let connector = TlsConnector::builder()
            .danger_accept_invalid_certs(true)
            .build()?;
        let (mut ws, _) = connect_async_tls_with_config(
            ws_url,
            None,
            true,
            Some(Connector::NativeTls(connector)),
        )
        .await?;

        let hello = self.wait_ws_control(&mut ws, "HELLO").await?;
        ws.send(Message::Text("{\"op\":\"PING\"}".into())).await?;
        let pong = self.wait_ws_control(&mut ws, "PONG").await?;

        let base = format!("{}websocket-delta", self.args.prefix);
        let dir = format!("{base}/dir");
        let file = format!("{base}/file.txt");
        let renamed = format!("{base}/renamed.txt");
        let payload = b"n1 websocket namespace delta\n".to_vec();
        let sha = sha256_bytes(&payload);

        self.posix_post("/n1/posix/mkdir", json!({"path": &dir}))
            .await?;
        let mkdir_event = self
            .wait_ws_namespace_event(&mut ws, "mkdir", Some(&dir), None)
            .await?;

        self.upload_bytes(&file, payload, "text/plain").await?;
        let upsert_event = self
            .wait_ws_namespace_event(&mut ws, "upsert_file", Some(&file), None)
            .await?;

        self.posix_post(
            "/n1/posix/rename",
            json!({"source_path": &file, "destination_path": &renamed}),
        )
        .await?;
        let rename_event = self
            .wait_ws_namespace_event(&mut ws, "rename", Some(&renamed), Some(&file))
            .await?;

        self.assert_download_sha256(&renamed, &sha).await?;
        expected.insert(renamed.clone());

        let trash = self
            .posix_post("/n1/posix/trash", json!({"path": &dir}))
            .await?;
        let trash_event = self
            .wait_ws_namespace_event(&mut ws, "trash", Some(&dir), None)
            .await?;
        let trash_entry_id = trash
            .get("trash_entry_id")
            .and_then(Value::as_str)
            .ok_or("websocket trash missing trash_entry_id")?;
        self.posix_post(
            "/n1/posix/recover",
            json!({"path": &dir, "trash_entry_id": trash_entry_id}),
        )
        .await?;
        let recover_event = self
            .wait_ws_namespace_event(&mut ws, "recover", Some(&dir), None)
            .await?;

        Ok(json!({
            "hello": hello,
            "pong": pong,
            "mkdir": mkdir_event,
            "upsert": upsert_event,
            "rename": rename_event,
            "trash": trash_event,
            "recover": recover_event,
        }))
    }

    async fn multipart_restart_resume(&mut self, object_key: &str) -> ProofResult<Value> {
        let total_size = self.args.source_file.metadata()?.len();
        let part_size = self.args.max_part_size;
        let expected_parts = total_size.div_ceil(part_size);
        let params = json!({
            "content_type": "application/octet-stream",
            "total_size_bytes": total_size,
            "expected_parts": expected_parts,
            "part_size_bytes": part_size,
        });
        let start = Instant::now();
        let init_url = format!(
            "{}/n1/upload/multipart/init/{}",
            self.base_url,
            encode_path(object_key)
        );
        let response = self
            .client
            .post(init_url)
            .bearer_auth(&self.token)
            .json(&params)
            .send()
            .await?;
        if response.status() != StatusCode::CREATED {
            return Err(format!(
                "multipart init failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let init: MultipartInit = response.json().await?;
        let restart_after_parts = (expected_parts / 2).max(1);
        self.upload_part_range(&init.version_id, 0, restart_after_parts, part_size)
            .await?;
        self.stop_server()?;
        self.start_server("resume").await?;
        self.auth().await?;
        self.upload_part_range(
            &init.version_id,
            restart_after_parts,
            expected_parts,
            part_size,
        )
        .await?;
        let finalize_url = format!(
            "{}/n1/upload/multipart/{}/{}/finalize?object_key={}",
            self.base_url,
            init.version_id,
            init.session_id,
            encode_query_value(object_key)
        );
        let response = self
            .client
            .post(finalize_url)
            .bearer_auth(&self.token)
            .json(&params)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "multipart finalize failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let elapsed = start.elapsed().as_secs_f64();
        Ok(json!({
            "mode": "multipart",
            "bytes": total_size,
            "parts": expected_parts,
            "part_size_bytes": part_size,
            "restart_after_parts": restart_after_parts,
            "elapsed_s": round3(elapsed),
            "mib_s": round3((total_size as f64 / 1024.0 / 1024.0) / elapsed.max(0.001)),
            "version_id": init.version_id,
            "session_id": init.session_id,
        }))
    }

    async fn multiple_large_multipart_proof(
        &mut self,
        expected: &mut BTreeSet<String>,
        expected_sha: &str,
    ) -> ProofResult<Value> {
        let file_name = self
            .args
            .source_file
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("large-object.bin")
            .to_string();
        let mut uploads = Vec::new();
        for idx in 1..self.args.large_copies {
            let object_key = format!(
                "{}large-multipart/copy-{idx:03}-{file_name}",
                self.args.prefix
            );
            let upload = self.multipart_restart_resume(&object_key).await?;
            expected.insert(object_key.clone());
            self.assert_list_contains(&object_key).await?;
            self.assert_download_sha256(&object_key, expected_sha)
                .await?;
            self.assert_object_status_clean(&object_key, "after_large_copy")
                .await?;
            uploads.push(json!({
                "object_key": object_key,
                "upload": upload,
            }));
        }
        Ok(json!({
            "requested_large_copies": self.args.large_copies,
            "additional_large_copies": uploads.len(),
            "uploads": uploads,
        }))
    }

    async fn concurrent_object_storm(
        &self,
        expected: &mut BTreeSet<String>,
        label: &str,
        count: usize,
    ) -> ProofResult<Value> {
        let start = Instant::now();
        let object_size = self.args.storm_object_size;
        let concurrency = self.args.storm_concurrency.max(1);
        let sample_stride = (count / 16).max(1);
        let keys = (0..count)
            .map(|idx| {
                format!(
                    "{}object-storm/{label}/tenant-{}/deep-{}/file-{idx:08}.bin",
                    self.args.prefix,
                    idx % 64,
                    idx % 257
                )
            })
            .collect::<Vec<_>>();
        let mut samples = Vec::new();
        let mut uploaded = 0usize;
        for chunk in keys.chunks(concurrency) {
            let uploads = chunk.iter().enumerate().map(|(local_idx, key)| {
                let idx = uploaded + local_idx;
                let key = key.clone();
                async move {
                    let payload = deterministic_payload(object_size, key.as_bytes());
                    let sha = sha256_bytes(&payload);
                    let version_id = self
                        .upload_bytes(&key, payload, "application/octet-stream")
                        .await?;
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>((idx, key, sha, version_id))
                }
            });
            let results = join_all(uploads).await;
            for result in results {
                let (idx, key, sha, version_id) = result?;
                expected.insert(key.clone());
                if idx == 0 || idx + 1 == count || idx % sample_stride == 0 {
                    samples.push(json!({
                        "index": idx,
                        "object_key": key,
                        "sha256": sha,
                        "version_id": version_id,
                    }));
                }
            }
            uploaded += chunk.len();
        }
        for sample in &samples {
            let object_key = sample
                .get("object_key")
                .and_then(Value::as_str)
                .ok_or("storm sample missing object_key")?;
            let sha = sample
                .get("sha256")
                .and_then(Value::as_str)
                .ok_or("storm sample missing sha256")?;
            self.assert_list_contains(object_key).await?;
            self.assert_download_sha256(object_key, sha).await?;
            self.assert_object_status_clean(object_key, "after_object_storm")
                .await?;
        }
        let elapsed = start.elapsed().as_secs_f64();
        Ok(json!({
            "label": label,
            "objects": count,
            "object_size_bytes": object_size,
            "concurrency": concurrency,
            "elapsed_s": round3(elapsed),
            "objects_per_s": round3(count as f64 / elapsed.max(0.001)),
            "mib_s": round3(((count * object_size) as f64 / 1024.0 / 1024.0) / elapsed.max(0.001)),
            "verified_samples": samples,
        }))
    }

    async fn same_fragment_mixed_user_proof(
        &self,
        expected: &mut BTreeSet<String>,
    ) -> ProofResult<Value> {
        let start = Instant::now();
        let users = self.args.mixed_users;
        let files_per_user = self.args.mixed_files_per_user;
        let base = format!("{}mixed-same-fragment", self.args.prefix);
        let reader = self.namespace_reader_probe(
            &self.token,
            (0..users)
                .map(|user| format!("{base}/user-{user:02}"))
                .collect(),
            96,
        );
        let actors = async {
            let futures = (0..users).map(|user| {
                let actor_base = format!("{base}/user-{user:02}");
                async move {
                    self.mixed_user_actor(&self.token, user, &actor_base, files_per_user)
                        .await
                }
            });
            let results = join_all(futures).await;
            let mut reports = Vec::new();
            let mut final_keys = Vec::new();
            for result in results {
                let actor = result?;
                final_keys.extend(actor.final_keys);
                reports.push(actor.report);
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((reports, final_keys))
        };
        let background = self.upload_one_shot_batch(
            &self.token,
            "same-fragment-background-upload",
            self.args.mixed_background_uploads,
            self.args.storm_object_size,
        );
        let (reader_report, actor_result, background_report) =
            tokio::try_join!(reader, actors, background)?;
        for key in actor_result.1 {
            expected.insert(key);
        }
        for key in background_report.final_keys {
            expected.insert(key);
        }
        Ok(json!({
            "users": users,
            "files_per_user": files_per_user,
            "background_uploads": self.args.mixed_background_uploads,
            "elapsed_s": round3(start.elapsed().as_secs_f64()),
            "reader": reader_report,
            "actors": actor_result.0,
            "background": background_report.report,
        }))
    }

    async fn parallel_multipart_user_proof(
        &self,
        expected: &mut BTreeSet<String>,
    ) -> ProofResult<Value> {
        let start = Instant::now();
        let users = self.args.mixed_users;
        let futures = (0..users).map(|user| async move {
            let object_key = format!(
                "{}parallel-multipart/user-{user:02}/object.bin",
                self.args.prefix
            );
            let payload = deterministic_payload(16 * 1024 * 1024, object_key.as_bytes());
            let sha = sha256_bytes(&payload);
            let report = self
                .multipart_bytes_with_token(&self.token, &object_key, payload, 2 * 1024 * 1024)
                .await?;
            self.assert_download_sha256(&object_key, &sha).await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(json!({
                "user": user,
                "object_key": object_key,
                "sha256": sha,
                "upload": report,
            }))
        });
        let results = join_all(futures).await;
        let mut reports = Vec::new();
        for result in results {
            let report = result?;
            if let Some(key) = report.get("object_key").and_then(Value::as_str) {
                expected.insert(key.to_string());
            }
            reports.push(report);
        }
        Ok(json!({
            "users": users,
            "elapsed_s": round3(start.elapsed().as_secs_f64()),
            "reports": reports,
        }))
    }

    async fn same_key_version_contention_proof(
        &self,
        expected: &mut BTreeSet<String>,
    ) -> ProofResult<Value> {
        let start = Instant::now();
        let object_key = format!("{}version-contention/shared-key.bin", self.args.prefix);
        let writes = self.args.mixed_users * 16;
        let concurrency = self.args.mixed_users.max(1);
        let mut known = BTreeSet::new();
        let mut version_ids = Vec::new();
        for chunk_start in (0..writes).step_by(concurrency) {
            let chunk_end = (chunk_start + concurrency).min(writes);
            let futures = (chunk_start..chunk_end).map(|idx| {
                let object_key = object_key.clone();
                let payload =
                    deterministic_payload(256 * 1024, format!("{object_key}:{idx}").as_bytes());
                let sha = sha256_bytes(&payload);
                async move {
                    let version_id = self
                        .upload_bytes(&object_key, payload, "application/octet-stream")
                        .await?;
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>((sha, version_id))
                }
            });
            for result in join_all(futures).await {
                let (sha, version_id) = result?;
                known.insert(sha);
                version_ids.push(version_id);
            }
        }
        self.assert_list_contains(&object_key).await?;
        let latest_sha = self.download_sha256(&object_key).await?;
        if !known.contains(&latest_sha) {
            return Err(format!(
                "same-key contention latest hash is not one of the committed versions: {latest_sha}"
            )
            .into());
        }
        expected.insert(object_key.clone());
        Ok(json!({
            "object_key": object_key,
            "writes": writes,
            "unique_payloads": known.len(),
            "version_ids": version_ids,
            "latest_sha256": latest_sha,
            "elapsed_s": round3(start.elapsed().as_secs_f64()),
        }))
    }

    async fn multi_fragment_isolation_proof(&self) -> ProofResult<Value> {
        let start = Instant::now();
        let users = self.args.multi_fragment_users;
        let files_per_user = self.args.multi_fragment_files_per_user;
        let quota_bytes = self.auxiliary_fragment_quota_bytes(files_per_user);
        let mut fragments = Vec::new();
        for user in 0..users {
            let key = format!("{}-mf-{user:02}", self.args.fragment_key);
            let name = format!("release-proof-mf-{}-{user:02}", self.stamp);
            let fragment = self.create_named_fragment(&name, &key, quota_bytes).await?;
            let token = self.auth_fragment(&fragment, &key).await?;
            fragments.push((user, fragment, token));
        }
        let futures = fragments.iter().map(|(user, fragment, token)| {
            let base = format!("{}multi-fragment/user-{user:02}", self.args.prefix);
            async move {
                let actor = self
                    .mixed_user_actor(token, *user, &base, files_per_user)
                    .await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(json!({
                    "user": user,
                    "fragment": fragment,
                    "actor": actor.report,
                }))
            }
        });
        let mut reports = Vec::new();
        for result in join_all(futures).await {
            reports.push(result?);
        }
        Ok(json!({
            "users": users,
            "files_per_user": files_per_user,
            "quota_bytes_per_fragment": quota_bytes,
            "elapsed_s": round3(start.elapsed().as_secs_f64()),
            "reports": reports,
        }))
    }

    async fn mixed_user_actor(
        &self,
        token: &str,
        user: usize,
        base: &str,
        files: usize,
    ) -> ProofResult<ActorReport> {
        let start = Instant::now();
        self.posix_post_with_token(
            token,
            "/n1/posix/mkdir",
            json!({"path": format!("{base}/inbox")}),
        )
        .await?;
        self.posix_post_with_token(
            token,
            "/n1/posix/mkdir",
            json!({"path": format!("{base}/work")}),
        )
        .await?;
        let sample_stride = (files / 8).max(1);
        let mut final_keys = Vec::with_capacity(files + 32);
        let mut samples = Vec::new();
        for idx in 0..files {
            let source = format!("{base}/inbox/file-{idx:08}.bin");
            let dest = format!("{base}/work/renamed-{idx:08}.bin");
            let payload = deterministic_payload(8 * 1024, format!("{base}:{idx}").as_bytes());
            let sha = sha256_bytes(&payload);
            self.upload_bytes_with_token(token, &source, payload, "application/octet-stream")
                .await?;
            self.posix_post_with_token(
                token,
                "/n1/posix/rename",
                json!({"source_path": source, "destination_path": dest}),
            )
            .await?;
            if idx == 0 || idx + 1 == files || idx % sample_stride == 0 {
                self.assert_list_contains_with_token(token, &dest).await?;
                self.assert_download_sha256_with_token(token, &dest, &sha)
                    .await?;
                samples.push(json!({"index": idx, "object_key": dest, "sha256": sha}));
            }
            final_keys.push(dest);
        }

        let folder_source = format!("{base}/folder-source");
        let folder_dest = format!("{base}/folder-dest");
        self.posix_post_with_token(token, "/n1/posix/mkdir", json!({"path": &folder_source}))
            .await?;
        for idx in 0..16usize {
            let child = format!("{folder_source}/child-{idx:04}.bin");
            let payload = deterministic_payload(32 * 1024, child.as_bytes());
            self.upload_bytes_with_token(token, &child, payload, "application/octet-stream")
                .await?;
        }
        self.posix_post_with_token(
            token,
            "/n1/posix/rename",
            json!({"source_path": folder_source, "destination_path": folder_dest}),
        )
        .await?;
        let moved_sample = format!("{folder_dest}/child-0000.bin");
        self.assert_list_contains_with_token(token, &moved_sample)
            .await?;
        for idx in 0..16usize {
            final_keys.push(format!("{folder_dest}/child-{idx:04}.bin"));
        }

        let recover_dir = format!("{base}/recover-dir");
        let recover_child = format!("{recover_dir}/payload.bin");
        let recover_sha = sha256_bytes(&deterministic_payload(64 * 1024, recover_child.as_bytes()));
        self.posix_post_with_token(token, "/n1/posix/mkdir", json!({"path": &recover_dir}))
            .await?;
        self.upload_bytes_with_token(
            token,
            &recover_child,
            deterministic_payload(64 * 1024, recover_child.as_bytes()),
            "application/octet-stream",
        )
        .await?;
        let trash = self
            .posix_post_with_token(token, "/n1/posix/trash", json!({"path": &recover_dir}))
            .await?;
        let trash_entry_id = trash
            .get("trash_entry_id")
            .and_then(Value::as_str)
            .ok_or("mixed user trash did not return trash_entry_id")?
            .to_string();
        self.posix_post_with_token(
            token,
            "/n1/posix/recover",
            json!({"path": recover_dir, "trash_entry_id": trash_entry_id}),
        )
        .await?;
        self.assert_download_sha256_with_token(token, &recover_child, &recover_sha)
            .await?;
        final_keys.push(recover_child.clone());

        let purge_dir = format!("{base}/purge-dir");
        let purge_child = format!("{purge_dir}/payload.bin");
        self.posix_post_with_token(token, "/n1/posix/mkdir", json!({"path": &purge_dir}))
            .await?;
        self.upload_bytes_with_token(
            token,
            &purge_child,
            deterministic_payload(32 * 1024, purge_child.as_bytes()),
            "application/octet-stream",
        )
        .await?;
        let trash = self
            .posix_post_with_token(token, "/n1/posix/trash", json!({"path": &purge_dir}))
            .await?;
        self.posix_post_with_token(
            token,
            "/n1/posix/purge",
            json!({
                "path": purge_dir,
                "trash_entry_id": trash.get("trash_entry_id").and_then(Value::as_str).unwrap_or(""),
            }),
        )
        .await?;

        Ok(ActorReport {
            final_keys,
            report: json!({
                "user": user,
                "base": base,
                "renamed_files": files,
                "folder_renamed_children": 16,
                "trash_recover_child": recover_child,
                "purged_folder": purge_dir,
                "verified_samples": samples,
                "elapsed_s": round3(start.elapsed().as_secs_f64()),
            }),
        })
    }

    async fn upload_one_shot_batch(
        &self,
        token: &str,
        label: &str,
        count: usize,
        object_size: usize,
    ) -> ProofResult<ActorReport> {
        let start = Instant::now();
        let concurrency = self.args.storm_concurrency.max(1);
        let mut final_keys = Vec::with_capacity(count);
        let mut samples = Vec::new();
        let sample_stride = (count / 8).max(1);
        for chunk_start in (0..count).step_by(concurrency) {
            let chunk_end = (chunk_start + concurrency).min(count);
            let futures = (chunk_start..chunk_end).map(|idx| {
                let key = format!("{}batch/{label}/object-{idx:08}.bin", self.args.prefix);
                let payload = deterministic_payload(object_size, key.as_bytes());
                let sha = sha256_bytes(&payload);
                async move {
                    let version_id = self
                        .upload_bytes_with_token(token, &key, payload, "application/octet-stream")
                        .await?;
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>((idx, key, sha, version_id))
                }
            });
            for result in join_all(futures).await {
                let (idx, key, sha, version_id) = result?;
                if idx == 0 || idx + 1 == count || idx % sample_stride == 0 {
                    self.assert_download_sha256_with_token(token, &key, &sha)
                        .await?;
                    samples.push(json!({
                        "index": idx,
                        "object_key": key,
                        "sha256": sha,
                        "version_id": version_id,
                    }));
                }
                final_keys.push(key);
            }
        }
        Ok(ActorReport {
            final_keys,
            report: json!({
                "label": label,
                "objects": count,
                "object_size_bytes": object_size,
                "concurrency": concurrency,
                "elapsed_s": round3(start.elapsed().as_secs_f64()),
                "verified_samples": samples,
            }),
        })
    }

    async fn namespace_reader_probe(
        &self,
        token: &str,
        bases: Vec<String>,
        rounds: usize,
    ) -> ProofResult<Value> {
        let start = Instant::now();
        let mut reads = 0usize;
        for _ in 0..rounds {
            for base in &bases {
                self.list_path_with_token(token, base, 200).await?;
                self.list_path_with_token(token, &format!("{base}/work"), 200)
                    .await?;
                reads += 2;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(json!({
            "bases": bases.len(),
            "rounds": rounds,
            "list_reads": reads,
            "elapsed_s": round3(start.elapsed().as_secs_f64()),
        }))
    }

    async fn multipart_bytes_with_token(
        &self,
        token: &str,
        object_key: &str,
        payload: Vec<u8>,
        part_size: usize,
    ) -> ProofResult<Value> {
        let start = Instant::now();
        let expected_parts = payload.len().div_ceil(part_size);
        let params = json!({
            "content_type": "application/octet-stream",
            "total_size_bytes": payload.len(),
            "expected_parts": expected_parts,
            "part_size_bytes": part_size,
        });
        let response = self
            .client
            .post(format!(
                "{}/n1/upload/multipart/init/{}",
                self.base_url,
                encode_path(object_key)
            ))
            .bearer_auth(token)
            .json(&params)
            .send()
            .await?;
        if response.status() != StatusCode::CREATED {
            return Err(format!(
                "parallel multipart init failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let init: MultipartInit = response.json().await?;
        for (idx, chunk) in payload.chunks(part_size).enumerate() {
            let response = self
                .client
                .put(format!(
                    "{}/n1/upload/multipart/{}/{}?durability=batched",
                    self.base_url, init.version_id, idx
                ))
                .bearer_auth(token)
                .header("content-type", "application/octet-stream")
                .body(chunk.to_vec())
                .send()
                .await?;
            if response.status() != StatusCode::OK && response.status() != StatusCode::CREATED {
                return Err(format!(
                    "parallel multipart part {idx} failed: {} {}",
                    response.status(),
                    response.text().await?
                )
                .into());
            }
        }
        let response = self
            .client
            .post(format!(
                "{}/n1/upload/multipart/{}/{}/finalize?object_key={}",
                self.base_url,
                init.version_id,
                init.session_id,
                encode_query_value(object_key)
            ))
            .bearer_auth(token)
            .json(&params)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "parallel multipart finalize failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(json!({
            "object_key": object_key,
            "bytes": payload.len(),
            "parts": expected_parts,
            "part_size_bytes": part_size,
            "version_id": init.version_id,
            "session_id": init.session_id,
            "elapsed_s": round3(start.elapsed().as_secs_f64()),
        }))
    }

    async fn list_path_with_token(
        &self,
        token: &str,
        path: &str,
        limit: usize,
    ) -> ProofResult<Value> {
        let response = self
            .client
            .get(format!(
                "{}/n1/posix/list?path={}&limit={}",
                self.base_url,
                encode_query_value(path),
                limit
            ))
            .bearer_auth(token)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "list probe failed for {path}: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(response.json().await?)
    }

    async fn wait_ws_control(
        &self,
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
        op: &str,
    ) -> ProofResult<Value> {
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut seen = Vec::new();
        while Instant::now() < deadline {
            let next = tokio::time::timeout(Duration::from_millis(500), ws.next()).await;
            let Some(message) = next.ok().flatten().transpose()? else {
                continue;
            };
            match message {
                Message::Text(text) => {
                    let value: Value = serde_json::from_str(text.as_ref())?;
                    seen.push(value.clone());
                    if value.get("op").and_then(Value::as_str) == Some(op) {
                        return Ok(value);
                    }
                }
                Message::Ping(bytes) => {
                    ws.send(Message::Pong(bytes)).await?;
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
        Err(format!("websocket did not emit control op={op}; seen={seen:?}").into())
    }

    async fn wait_ws_namespace_event(
        &self,
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
        operation: &str,
        path: Option<&str>,
        source_path: Option<&str>,
    ) -> ProofResult<Value> {
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut seen = Vec::new();
        while Instant::now() < deadline {
            let next = tokio::time::timeout(Duration::from_millis(500), ws.next()).await;
            let Some(message) = next.ok().flatten().transpose()? else {
                continue;
            };
            match message {
                Message::Text(text) => {
                    let value: Value = serde_json::from_str(text.as_ref())?;
                    seen.push(value.clone());
                    let is_event = value.get("op").and_then(Value::as_str) == Some("EVENT");
                    let data = value
                        .get("d")
                        .and_then(|event| event.get("data"))
                        .cloned()
                        .unwrap_or_else(|| json!(null));
                    let event_operation = data.get("operation").and_then(Value::as_str);
                    let event_path = data.get("path").and_then(Value::as_str);
                    let event_source = data.get("source_path").and_then(Value::as_str);
                    let event_dest = data.get("destination_path").and_then(Value::as_str);
                    let matches_operation =
                        event_operation.is_some_and(|actual| actual.contains(operation));
                    let matches_path = path.is_none_or(|wanted| {
                        event_path == Some(wanted) || event_dest == Some(wanted)
                    });
                    let matches_source =
                        source_path.is_none_or(|wanted| event_source == Some(wanted));
                    if is_event && matches_operation && matches_path && matches_source {
                        return Ok(data);
                    }
                }
                Message::Ping(bytes) => {
                    ws.send(Message::Pong(bytes)).await?;
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
        Err(format!(
            "websocket did not emit operation={operation} path={path:?} source={source_path:?}; seen={seen:?}"
        )
        .into())
    }

    fn auxiliary_fragment_quota_bytes(&self, files_per_user: usize) -> u64 {
        let estimated_payload = (files_per_user as u64 * 8 * 1024)
            + (16 * 32 * 1024)
            + (64 * 1024)
            + (32 * 1024)
            + (256 * 1024 * 1024);
        estimated_payload.max(512 * 1024 * 1024)
    }

    async fn create_named_fragment(
        &self,
        name: &str,
        key: &str,
        quota_bytes: u64,
    ) -> ProofResult<String> {
        let response = self
            .client
            .post(format!("{}/n1/fragments", self.base_url))
            .json(&json!({
                "name": name,
                "secret": self.args.master_key,
                "key": key,
                "quota_bytes": quota_bytes,
            }))
            .send()
            .await?;
        if response.status() != StatusCode::CREATED {
            return Err(format!(
                "multi-fragment create failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(response.json::<FragmentCreate>().await?.fragment_hash)
    }

    async fn auth_fragment(&self, fragment: &str, key: &str) -> ProofResult<String> {
        let response = self
            .client
            .post(format!("{}/n1/auth/fragment", self.base_url))
            .json(&json!({
                "fragment": fragment,
                "secret": key,
                "read": true,
                "write": true,
                "modify": true,
                "delete": true,
                "expires_in": 21600,
            }))
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "multi-fragment auth failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(response.json::<AuthResponse>().await?.access_token)
    }

    async fn upload_part_range(
        &self,
        version_id: &str,
        start_part: u64,
        end_part: u64,
        part_size: u64,
    ) -> ProofResult<()> {
        let total_size = self.args.source_file.metadata()?.len();
        for part_idx in start_part..end_part {
            let offset = part_idx * part_size;
            let remaining = total_size.saturating_sub(offset);
            let len = remaining.min(part_size);
            let mut file = tokio::fs::File::open(&self.args.source_file).await?;
            file.seek(std::io::SeekFrom::Start(offset)).await?;
            let reader = file.take(len);
            let stream = ReaderStream::with_capacity(reader, 1024 * 1024);
            let response = self
                .client
                .put(format!(
                    "{}/n1/upload/multipart/{}/{}?durability=batched",
                    self.base_url, version_id, part_idx
                ))
                .bearer_auth(&self.token)
                .header("content-type", "application/octet-stream")
                .header(CONTENT_LENGTH, len)
                .body(reqwest::Body::wrap_stream(stream))
                .send()
                .await?;
            if response.status() != StatusCode::OK && response.status() != StatusCode::CREATED {
                return Err(format!(
                    "multipart part {part_idx} failed: {} {}",
                    response.status(),
                    response.text().await?
                )
                .into());
            }
        }
        Ok(())
    }

    async fn assert_list_contains(&self, object_key: &str) -> ProofResult<()> {
        self.assert_list_contains_with_token(&self.token, object_key)
            .await
    }

    async fn assert_list_contains_with_token(
        &self,
        token: &str,
        object_key: &str,
    ) -> ProofResult<()> {
        let (parent, name) = split_parent_name(object_key);
        let mut continuation_token: Option<String> = None;
        let mut pages = 0usize;
        let mut last_names = Vec::new();

        loop {
            pages += 1;
            if pages > 10_000 {
                return Err(format!(
                    "list pagination exceeded 10000 pages while looking for {object_key} under {parent}"
                )
                .into());
            }

            let mut url = format!(
                "{}/n1/posix/list?path={}&limit=1000",
                self.base_url,
                encode_query_value(parent)
            );
            if let Some(token_value) = continuation_token.as_deref() {
                url.push_str("&continuation_token=");
                url.push_str(&encode_query_value(token_value));
            }

            let response = self.client.get(url).bearer_auth(token).send().await?;
            if response.status() != StatusCode::OK {
                return Err(format!(
                    "list failed for {object_key}: {} {}",
                    response.status(),
                    response.text().await?
                )
                .into());
            }

            let body: Value = response.json().await?;
            let entries = body.get("entries").and_then(Value::as_array);
            if let Some(entries) = entries {
                if entries
                    .iter()
                    .any(|entry| entry.get("name").and_then(Value::as_str) == Some(name))
                {
                    return Ok(());
                }

                last_names = entries
                    .iter()
                    .filter_map(|entry| entry.get("name").and_then(Value::as_str))
                    .take(8)
                    .map(str::to_owned)
                    .collect();
            }

            continuation_token = body
                .get("continuation_token")
                .and_then(Value::as_str)
                .map(str::to_owned);
            if continuation_token.is_none() {
                return Err(format!(
                    "list did not contain {object_key} after {pages} page(s) under {parent}; last_names={last_names:?}"
                )
                .into());
            }
        }
    }

    async fn download_sha256(&self, object_key: &str) -> ProofResult<String> {
        self.download_sha256_with_token(&self.token, object_key)
            .await
    }

    async fn download_sha256_with_token(
        &self,
        token: &str,
        object_key: &str,
    ) -> ProofResult<String> {
        let response = self
            .client
            .get(format!(
                "{}/n1/objects/download?object_key={}",
                self.base_url,
                encode_query_value(object_key)
            ))
            .bearer_auth(token)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "download failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let mut hasher = Sha256::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            hasher.update(&chunk?);
        }
        Ok(format!("{:x}", hasher.finalize()))
    }

    async fn expect_download_status(
        &self,
        object_key: &str,
        expected: StatusCode,
    ) -> ProofResult<()> {
        let response = self
            .client
            .get(format!(
                "{}/n1/objects/download?object_key={}",
                self.base_url,
                encode_query_value(object_key)
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != expected {
            return Err(format!(
                "download status mismatch for {object_key}: expected={} actual={} body={}",
                expected,
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(())
    }

    async fn range_probe(&self, object_key: &str) -> ProofResult<()> {
        let response = self
            .client
            .get(format!(
                "{}/n1/objects/download?object_key={}",
                self.base_url,
                encode_query_value(object_key)
            ))
            .bearer_auth(&self.token)
            .header("range", "bytes=0-1048575")
            .send()
            .await?;
        if response.status() != StatusCode::PARTIAL_CONTENT {
            return Err(format!(
                "range probe failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let bytes = response.bytes().await?;
        if bytes.is_empty() || bytes.len() > 1_048_576 {
            return Err(
                format!("range probe returned unexpected byte count {}", bytes.len()).into(),
            );
        }
        Ok(())
    }

    async fn assert_download_sha256(&self, object_key: &str, expected: &str) -> ProofResult<()> {
        self.assert_download_sha256_with_token(&self.token, object_key, expected)
            .await
    }

    async fn assert_download_sha256_with_token(
        &self,
        token: &str,
        object_key: &str,
        expected: &str,
    ) -> ProofResult<()> {
        let actual = self.download_sha256_with_token(token, object_key).await?;
        if actual != expected {
            return Err(format!(
                "download hash mismatch for {object_key}: expected={expected} actual={actual}"
            )
            .into());
        }
        Ok(())
    }

    async fn upload_bytes(
        &self,
        object_key: &str,
        payload: Vec<u8>,
        content_type: &str,
    ) -> ProofResult<String> {
        self.upload_bytes_with_token(&self.token, object_key, payload, content_type)
            .await
    }

    async fn upload_bytes_with_token(
        &self,
        token: &str,
        object_key: &str,
        payload: Vec<u8>,
        content_type: &str,
    ) -> ProofResult<String> {
        let response = self
            .client
            .post(format!(
                "{}/n1/upload/one-shot/{}",
                self.base_url,
                encode_path(object_key)
            ))
            .bearer_auth(token)
            .header("content-type", content_type)
            .body(payload)
            .send()
            .await?;
        if response.status() != StatusCode::CREATED {
            return Err(format!(
                "one-shot upload failed for {object_key}: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let body: Value = response.json().await?;
        if body.get("object_key").and_then(Value::as_str) != Some(object_key) {
            return Err(format!("one-shot logical path mismatch for {object_key}: {body}").into());
        }
        Ok(body
            .get("version_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string())
    }

    async fn posix_post(&self, route: &str, body: Value) -> ProofResult<Value> {
        self.posix_post_with_token(&self.token, route, body).await
    }

    async fn posix_post_with_token(
        &self,
        token: &str,
        route: &str,
        body: Value,
    ) -> ProofResult<Value> {
        let response = self
            .client
            .post(format!("{}{}", self.base_url, route))
            .bearer_auth(token)
            .json(&body)
            .send()
            .await?;
        if !matches!(
            response.status(),
            StatusCode::OK | StatusCode::CREATED | StatusCode::ACCEPTED
        ) {
            return Err(format!(
                "{route} failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(response.json().await?)
    }

    async fn posix_stat(&self, object_key: &str, trash: bool) -> ProofResult<Value> {
        let response = self
            .client
            .get(format!(
                "{}/n1/posix/stat?path={}&trash={}",
                self.base_url,
                encode_query_value(object_key),
                trash
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "posix stat failed for {object_key}: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(response.json().await?)
    }

    async fn assert_expected_visible(&self, expected: &BTreeSet<String>) -> ProofResult<()> {
        for object_key in expected {
            self.posix_stat(object_key, false).await?;
        }
        Ok(())
    }

    async fn posix_crud_gc_proof(&self, expected: &mut BTreeSet<String>) -> ProofResult<Value> {
        let start = Instant::now();
        let base = format!("{}n1-crud", self.args.prefix);
        self.posix_post("/n1/posix/mkdir", json!({"path": format!("{base}/folder")}))
            .await?;

        let old_file = format!("{base}/rename-me.txt");
        let new_file = format!("{base}/renamed.txt");
        let rename_payload = b"n1 rename payload\n".to_vec();
        let rename_sha = sha256_bytes(&rename_payload);
        self.upload_bytes(&old_file, rename_payload, "text/plain")
            .await?;
        self.posix_post(
            "/n1/posix/rename",
            json!({"source_path": old_file, "destination_path": new_file}),
        )
        .await?;
        self.assert_download_sha256(&new_file, &rename_sha).await?;
        expected.insert(new_file.clone());

        let child = format!("{base}/folder/child.txt");
        let moved_child = format!("{base}/folder-renamed/child.txt");
        let child_payload = b"n1 folder rename payload\n".to_vec();
        let child_sha = sha256_bytes(&child_payload);
        self.upload_bytes(&child, child_payload, "text/plain")
            .await?;
        self.posix_post(
            "/n1/posix/rename",
            json!({"source_path": format!("{base}/folder"), "destination_path": format!("{base}/folder-renamed")}),
        )
        .await?;
        self.assert_download_sha256(&moved_child, &child_sha)
            .await?;

        let trash = self
            .posix_post(
                "/n1/posix/trash",
                json!({"path": format!("{base}/folder-renamed")}),
            )
            .await?;
        let trash_entry_id = trash
            .get("trash_entry_id")
            .and_then(Value::as_str)
            .ok_or("trash did not return trash_entry_id")?
            .to_string();
        self.posix_stat(&format!("{base}/folder-renamed"), true)
            .await?;
        self.posix_post(
            "/n1/posix/recover",
            json!({"path": format!("{base}/folder-renamed"), "trash_entry_id": trash_entry_id}),
        )
        .await?;
        self.assert_download_sha256(&moved_child, &child_sha)
            .await?;
        expected.insert(moved_child.clone());

        let delete_me = format!("{base}/delete-me.bin");
        let delete_payload = deterministic_payload(256 * 1024, b"n1-gc-purge");
        let version_id = self
            .upload_bytes(&delete_me, delete_payload, "application/octet-stream")
            .await?;
        let trash = self
            .posix_post("/n1/posix/trash", json!({"path": delete_me}))
            .await?;
        self.posix_post(
            "/n1/posix/purge",
            json!({"path": delete_me, "trash_entry_id": trash.get("trash_entry_id").and_then(Value::as_str).unwrap_or("")}),
        )
        .await?;
        self.run_gc_and_wait().await?;
        if self.blob_version_exists(&version_id) {
            return Err(format!("GC did not reclaim purged version {version_id}").into());
        }
        Ok(json!({
            "elapsed_s": round3(start.elapsed().as_secs_f64()),
            "renamed_file": new_file,
            "recovered_folder_child": moved_child,
            "gc_reclaimed_version": version_id,
        }))
    }

    async fn small_multipart_proof(&self, expected: &mut BTreeSet<String>) -> ProofResult<Value> {
        let logical = format!("{}n1-multipart/video-parted.bin", self.args.prefix);
        let payload = deterministic_payload(8 * 1024 * 1024, b"n1-multipart-proof");
        let payload_sha = sha256_bytes(&payload);
        let part_size = 2 * 1024 * 1024_u64;
        let expected_parts = 4_u64;
        let params = json!({
            "content_type": "application/octet-stream",
            "total_size_bytes": payload.len(),
            "expected_parts": expected_parts,
            "part_size_bytes": part_size,
        });
        let start = Instant::now();
        let response = self
            .client
            .post(format!(
                "{}/n1/upload/multipart/init/{}",
                self.base_url,
                encode_path(&logical)
            ))
            .bearer_auth(&self.token)
            .json(&params)
            .send()
            .await?;
        if response.status() != StatusCode::CREATED {
            return Err(format!(
                "small multipart init failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let init: MultipartInit = response.json().await?;
        for idx in 0..expected_parts {
            let begin = idx as usize * part_size as usize;
            let end = begin + part_size as usize;
            let response = self
                .client
                .put(format!(
                    "{}/n1/upload/multipart/{}/{}?durability=batched",
                    self.base_url, init.version_id, idx
                ))
                .bearer_auth(&self.token)
                .header("content-type", "application/octet-stream")
                .body(payload[begin..end].to_vec())
                .send()
                .await?;
            if response.status() != StatusCode::OK && response.status() != StatusCode::CREATED {
                return Err(format!(
                    "small multipart part {idx} failed: {} {}",
                    response.status(),
                    response.text().await?
                )
                .into());
            }
        }
        let response = self
            .client
            .post(format!(
                "{}/n1/upload/multipart/{}/{}/finalize?object_key={}",
                self.base_url,
                init.version_id,
                init.session_id,
                encode_query_value(&logical)
            ))
            .bearer_auth(&self.token)
            .json(&params)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "small multipart finalize failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        self.assert_download_sha256(&logical, &payload_sha).await?;
        expected.insert(logical);
        Ok(json!({
            "bytes": payload.len(),
            "parts": expected_parts,
            "elapsed_s": round3(start.elapsed().as_secs_f64()),
            "version_id": init.version_id,
        }))
    }

    async fn archive_proof(&self, expected: &mut BTreeSet<String>) -> ProofResult<Value> {
        let archive_prefix = format!("{}archive-proof", self.args.prefix);
        let mut archive_expected = BTreeSet::new();
        for idx in 0..3 {
            let key = format!("{archive_prefix}/file-{idx:02}.txt");
            let payload = deterministic_payload(32 * 1024, key.as_bytes());
            let sha = sha256_bytes(&payload);
            self.upload_bytes(&key, payload, "text/plain").await?;
            self.assert_download_sha256(&key, &sha).await?;
            expected.insert(key.clone());
            archive_expected.insert(key);
        }

        let response = self
            .client
            .post(format!("{}/n1/objects/archive", self.base_url))
            .bearer_auth(&self.token)
            .json(
                &json!({"prefixes": [archive_prefix], "format": "tar", "filename": "n1-proof.tar"}),
            )
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "archive failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let raw = response_bytes_limited(response, 64 * 1024 * 1024, "archive").await?;
        let names = tar_names(&raw)?;
        for sample in &archive_expected {
            if !names.contains(sample) {
                return Err(format!("archive missing sample {sample}; names={names:?}").into());
            }
        }
        Ok(json!({
            "bytes": raw.len(),
            "members": names.len(),
            "samples": archive_expected,
        }))
    }

    async fn simulate_recover_on_read(
        &self,
        object_key: &str,
        expected_sha: &str,
    ) -> ProofResult<Value> {
        let version_id = self.latest_version_id(object_key).await?;
        let removed = self.remove_one_shard(&version_id)?;
        self.assert_download_sha256(object_key, expected_sha)
            .await?;
        self.wait_for_path(&removed, true, Duration::from_secs(30))
            .await?;
        Ok(json!({"object_key": object_key, "version_id": version_id, "restored": removed}))
    }

    async fn object_repair_status_proof(&self, object_key: &str) -> ProofResult<Value> {
        let version_id = self.latest_version_id(object_key).await?;
        let removed = self.remove_one_shard(&version_id)?;
        let degraded = self.object_status(object_key, "strict").await?;
        let status = degraded
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        if status == "healthy" {
            return Err(
                format!("object status stayed healthy after shard removal: {degraded}").into(),
            );
        }
        let response = self
            .client
            .post(format!(
                "{}/n1/objects/repair/{}?mode=strict",
                self.base_url,
                encode_path(object_key)
            ))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|err| format!("object repair request failed for {object_key}: {err:?}"))?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "object repair failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        self.wait_for_path(&removed, true, Duration::from_secs(30))
            .await?;
        self.assert_object_status_clean(object_key, "after_object_repair")
            .await?;
        Ok(
            json!({"object_key": object_key, "version_id": version_id, "removed": removed, "pre_repair_status": degraded}),
        )
    }

    async fn simulate_disk_format_and_repair(
        &mut self,
        expected: &BTreeSet<String>,
        sample_key: &str,
        sample_sha: &str,
    ) -> ProofResult<Value> {
        self.stop_server()?;
        let removed_index = self.index_roots[0].join(&self.fragment);
        let removed_blob = self.blob_roots[0].join(&self.fragment);
        let _ = fs::remove_dir_all(&removed_index);
        let _ = fs::remove_dir_all(&removed_blob);
        self.start_server("disk-format").await?;
        self.auth().await?;
        self.run_repair_and_wait().await?;
        self.assert_expected_visible(expected).await?;
        self.assert_download_sha256(sample_key, sample_sha).await?;
        self.assert_health_clean("after_disk_format_repair").await?;
        Ok(json!({"removed_index": removed_index, "removed_blob": removed_blob}))
    }

    async fn redistribution_proof(
        &mut self,
        expected: &mut BTreeSet<String>,
    ) -> ProofResult<Value> {
        self.stop_server()?;
        self.active_disk_count = 1;
        self.parity_shards = 0;
        self.write_config()?;
        self.start_server("one-disk").await?;
        self.auth().await?;
        self.assert_expected_visible(expected).await?;
        self.run_redistribution_and_wait().await?;

        let one_disk_key = format!("{}n1-redistribution/one-disk.txt", self.args.prefix);
        let payload = b"n1 one disk redistribution payload\n".to_vec();
        let sha = sha256_bytes(&payload);
        self.upload_bytes(&one_disk_key, payload, "text/plain")
            .await?;
        self.assert_download_sha256(&one_disk_key, &sha).await?;
        expected.insert(one_disk_key.clone());

        self.stop_server()?;
        self.active_disk_count = self.index_roots.len();
        self.parity_shards = 1;
        self.write_config()?;
        self.start_server("two-disk-again").await?;
        self.auth().await?;
        self.run_redistribution_and_wait().await?;
        self.run_repair_and_wait().await?;
        self.assert_expected_visible(expected).await?;
        self.assert_download_sha256(&one_disk_key, &sha).await?;
        self.assert_health_clean("after_redistribution").await?;
        Ok(json!({"one_disk_object": one_disk_key, "active_disk_count": self.active_disk_count}))
    }

    async fn chaos_proof(
        &mut self,
        expected: &mut BTreeSet<String>,
        large_sha: &str,
        rss_samples: &mut Vec<Value>,
    ) -> ProofResult<Value> {
        let start = Instant::now();
        let min_cycles = if self.args.chaos_cycles == 0 {
            1
        } else {
            self.args.chaos_cycles
        };
        let deadline = if self.args.chaos_seconds == 0 {
            None
        } else {
            Some(start + Duration::from_secs(self.args.chaos_seconds))
        };
        let file_name = self
            .args
            .source_file
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("large-object.bin")
            .to_string();
        let mut cycles = Vec::new();
        let mut cycle_idx = 0usize;
        loop {
            let cycle_start = Instant::now();
            let storm = self
                .concurrent_object_storm(
                    expected,
                    &format!("chaos-cycle-{cycle_idx:03}"),
                    self.args.chaos_storm_objects,
                )
                .await?;
            rss_samples.push(self.rss_sample(&format!("chaos_{cycle_idx:03}_after_storm")));

            let large_key = format!(
                "{}chaos/cycle-{cycle_idx:03}/multipart-{file_name}",
                self.args.prefix
            );
            let upload = self.multipart_restart_resume(&large_key).await?;
            expected.insert(large_key.clone());
            self.assert_download_sha256(&large_key, large_sha).await?;
            rss_samples.push(self.rss_sample(&format!("chaos_{cycle_idx:03}_after_large")));

            let version_id = self.latest_version_id(&large_key).await?;
            let removed = self.remove_one_shard(&version_id)?;
            self.assert_download_sha256(&large_key, large_sha).await?;
            self.wait_for_path(&removed, true, Duration::from_secs(60))
                .await?;
            self.assert_object_status_clean(&large_key, "chaos_recover_on_read")
                .await?;

            let trash_root = format!("{}chaos-trash/cycle-{cycle_idx:03}", self.args.prefix);
            let trash_child = format!("{trash_root}/payload.txt");
            let payload = deterministic_payload(128 * 1024, trash_child.as_bytes());
            let trash_sha = sha256_bytes(&payload);
            self.posix_post("/n1/posix/mkdir", json!({"path": trash_root}))
                .await?;
            self.upload_bytes(&trash_child, payload, "text/plain")
                .await?;
            expected.insert(trash_child.clone());
            let trash = self
                .posix_post("/n1/posix/trash", json!({"path": trash_root}))
                .await?;
            expected.remove(&trash_child);
            let trash_entry_id = trash
                .get("trash_entry_id")
                .and_then(Value::as_str)
                .ok_or("chaos trash did not return trash_entry_id")?
                .to_string();
            self.posix_post(
                "/n1/posix/recover",
                json!({"path": trash_root, "trash_entry_id": trash_entry_id}),
            )
            .await?;
            self.assert_download_sha256(&trash_child, &trash_sha)
                .await?;
            expected.insert(trash_child.clone());

            if cycle_idx == 0 {
                let repair = self
                    .simulate_disk_format_and_repair(expected, &large_key, large_sha)
                    .await?;
                rss_samples.push(self.rss_sample("chaos_after_disk_wipe_repair"));
                cycles.push(json!({
                    "cycle": cycle_idx,
                    "storm": storm,
                    "large_multipart": upload,
                    "recover_on_read_version_id": version_id,
                    "disk_wipe_repair": repair,
                    "trash_recover_object": trash_child,
                    "elapsed_s": round3(cycle_start.elapsed().as_secs_f64()),
                }));
            } else {
                cycles.push(json!({
                    "cycle": cycle_idx,
                    "storm": storm,
                    "large_multipart": upload,
                    "recover_on_read_version_id": version_id,
                    "trash_recover_object": trash_child,
                    "elapsed_s": round3(cycle_start.elapsed().as_secs_f64()),
                }));
            }

            self.run_gc_and_wait().await?;
            self.assert_health_clean(&format!("chaos_cycle_{cycle_idx}"))
                .await?;
            cycle_idx += 1;
            let reached_min_cycles = cycle_idx >= min_cycles;
            let reached_deadline = deadline.is_none_or(|limit| Instant::now() >= limit);
            if reached_min_cycles && reached_deadline {
                break;
            }
        }
        Ok(json!({
            "enabled": true,
            "cycles": cycles.len(),
            "chaos_seconds_requested": self.args.chaos_seconds,
            "chaos_cycles_requested": self.args.chaos_cycles,
            "elapsed_s": round3(start.elapsed().as_secs_f64()),
            "cycle_reports": cycles,
        }))
    }

    async fn object_status(&self, object_key: &str, mode: &str) -> ProofResult<Value> {
        let response = self
            .client
            .get(format!(
                "{}/n1/objects/status/{}?mode={}",
                self.base_url,
                encode_path(object_key),
                mode
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "object status failed for {object_key}: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(response.json().await?)
    }

    async fn assert_object_status_clean(&self, object_key: &str, phase: &str) -> ProofResult<()> {
        let body = self.object_status(object_key, "strict").await?;
        let status = body
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        if status != "healthy" {
            return Err(format!("{phase} object status is not healthy: {body}").into());
        }
        Ok(())
    }

    async fn assert_health_clean(&self, phase: &str) -> ProofResult<()> {
        let response = self
            .client
            .get(format!(
                "{}/n1/maintenance/health?deep_scan=true",
                self.base_url
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "{phase} health failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let body: Value = response.json().await?;
        let mut bad = Vec::new();
        collect_unhealthy_strings("", &body, &mut bad);
        if !bad.is_empty() {
            return Err(format!("{phase} health reported unhealthy state: {:?}", bad).into());
        }
        Ok(())
    }

    async fn run_gc_and_wait(&self) -> ProofResult<Value> {
        let response = self
            .client
            .post(format!(
                "{}/n1/maintenance/fragments/{}/gc/start?mode=scrub&fast_mode=true",
                self.base_url, self.fragment
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != StatusCode::ACCEPTED {
            return Err(format!(
                "GC start failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        self.wait_job("/n1/maintenance/gc/status", "GC").await
    }

    async fn run_repair_and_wait(&self) -> ProofResult<Value> {
        let response = self
            .client
            .post(format!(
                "{}/n1/maintenance/fragments/{}/repair/start?mode=scrub&fast_mode=true&meta_concurrency=4&repair_concurrency=2&batch=128",
                self.base_url, self.fragment
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != StatusCode::ACCEPTED {
            return Err(format!(
                "repair start failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        self.wait_job("/n1/maintenance/repair/status", "repair")
            .await
    }

    async fn run_redistribution_and_wait(&self) -> ProofResult<Value> {
        let response = self
            .client
            .post(format!(
                "{}/n1/maintenance/fragments/{}/redistribution/start?mode=scrub&fast_mode=true&durability=batched&object_concurrency=8&status_batch=512",
                self.base_url, self.fragment
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != StatusCode::ACCEPTED {
            return Err(format!(
                "redistribution start failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        self.wait_job("/n1/maintenance/redistribution/status", "redistribution")
            .await
    }

    async fn wait_job(&self, path: &str, label: &str) -> ProofResult<Value> {
        let deadline = Instant::now() + Duration::from_secs(self.args.job_timeout_seconds);
        let mut last = json!(null);
        while Instant::now() < deadline {
            let response = self
                .client
                .get(format!("{}{}", self.base_url, path))
                .bearer_auth(&self.token)
                .send()
                .await?;
            if response.status() != StatusCode::OK {
                return Err(format!(
                    "{label} status failed: {} {}",
                    response.status(),
                    response.text().await?
                )
                .into());
            }
            last = response.json().await?;
            if !last
                .get("is_running")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                let phase = last
                    .get("phase")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                let last_error = last.get("last_error").filter(|value| !value.is_null());
                if phase == "failed" || last_error.is_some() {
                    return Err(format!("{label} failed: {last}").into());
                }
                return Ok(last);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Err(format!("{label} timed out; last={last}").into())
    }

    async fn list_version_ids(&self, object_key: &str) -> ProofResult<Vec<String>> {
        let response = self
            .client
            .get(format!(
                "{}/n1/objects/versions/{}",
                self.base_url,
                encode_path(object_key)
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "versions failed for {object_key}: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let versions: Value = response.json().await?;
        let ids = versions
            .as_array()
            .ok_or_else(|| {
                format!("versions response was not an array for {object_key}: {versions}")
            })?
            .iter()
            .filter_map(|item| item.get("version_id").and_then(Value::as_str))
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        if ids.is_empty() {
            return Err(format!("no versions for {object_key}: {versions}").into());
        }
        Ok(ids)
    }

    async fn latest_version_id(&self, object_key: &str) -> ProofResult<String> {
        self.list_version_ids(object_key)
            .await?
            .into_iter()
            .max()
            .ok_or_else(|| format!("no versions for {object_key}").into())
    }

    fn remove_one_shard(&self, version_id: &str) -> ProofResult<PathBuf> {
        let version_dir = self.blob_roots[0]
            .join(&self.fragment)
            .join(".blobs")
            .join(version_id);
        let mut shard_files = fs::read_dir(&version_dir)?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_file())
            .collect::<Vec<_>>();
        shard_files.sort();
        let removed = shard_files
            .into_iter()
            .next()
            .ok_or_else(|| format!("no shard files under {}", version_dir.display()))?;
        fs::remove_file(&removed)?;
        Ok(removed)
    }

    async fn wait_for_path(
        &self,
        path: &Path,
        should_exist: bool,
        timeout: Duration,
    ) -> ProofResult<()> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if path.exists() == should_exist {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        Err(format!(
            "path {} did not reach existence={} within {:?}",
            path.display(),
            should_exist,
            timeout
        )
        .into())
    }

    fn blob_version_exists(&self, version_id: &str) -> bool {
        self.blob_roots.iter().any(|root| {
            root.join(&self.fragment)
                .join(".blobs")
                .join(version_id)
                .exists()
        })
    }

    fn inspect_index(&self, expected_live_objects: usize) -> ProofResult<Value> {
        let mut inspections = Vec::new();
        for root in &self.index_roots {
            let fragment_root = root.join(&self.fragment);
            if fragment_root.join("indexdb").exists() {
                return Err(format!(
                    "N1 fragment must not contain indexdb: {}",
                    fragment_root.display()
                )
                .into());
            }
            let object_root = fragment_root.join(".objects");
            let live_ndx = count_files_with_suffix(&object_root, ".ndx")?;
            let object_record_dirs = count_object_record_dirs(&object_root)?;
            let namespace_symlinks = count_symlinks_excluding(&fragment_root, ".trash")?;
            let blob_root = self
                .blob_roots
                .get(
                    self.index_roots
                        .iter()
                        .position(|value| value == root)
                        .unwrap_or(0),
                )
                .unwrap_or(&self.blob_roots[0]);
            let blob_version_dirs =
                count_dirs(blob_root.join(&self.fragment).join(".blobs").as_path())?;
            if object_record_dirs != expected_live_objects {
                return Err(format!(
                    "index inspection failed under {}: object_record_dirs={} expected={}",
                    fragment_root.display(),
                    object_record_dirs,
                    expected_live_objects
                )
                .into());
            }
            if namespace_symlinks != expected_live_objects {
                return Err(format!(
                    "index inspection failed under {}: namespace_symlinks={} expected={}",
                    fragment_root.display(),
                    namespace_symlinks,
                    expected_live_objects
                )
                .into());
            }
            if live_ndx < expected_live_objects {
                return Err(format!(
                    "index inspection failed under {}: live_ndx={} expected_at_least={}",
                    fragment_root.display(),
                    live_ndx,
                    expected_live_objects
                )
                .into());
            }
            if blob_version_dirs < expected_live_objects {
                return Err(format!(
                    "blob inspection failed under {}: blob_version_dirs={} expected_at_least={}",
                    blob_root.display(),
                    blob_version_dirs,
                    expected_live_objects
                )
                .into());
            }
            inspections.push(json!({
                "index_root": root,
                "blob_root": blob_root,
                "object_record_dirs": object_record_dirs,
                "live_ndx": live_ndx,
                "namespace_symlinks": namespace_symlinks,
                "blob_version_dirs": blob_version_dirs,
            }));
        }
        Ok(json!(inspections))
    }

    async fn sha256_file(&self, path: &Path) -> ProofResult<String> {
        let mut file = tokio::fs::File::open(path).await?;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; 1024 * 1024];
        loop {
            let read = file.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        Ok(format!("{:x}", hasher.finalize()))
    }

    fn object_key(&self) -> String {
        let name = self
            .args
            .source_file
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("large-object.bin");
        format!("{}{}", self.args.prefix, name)
    }

    fn rss_sample(&self, label: &str) -> Value {
        let pid = self.server.as_ref().map(Child::id);
        let rss_kib = pid.and_then(|pid| read_rss_kib(pid).ok().flatten());
        json!({
            "label": label,
            "pid": pid,
            "rss_kib": rss_kib,
        })
    }

    fn scan_logs(&self) -> ProofResult<Value> {
        let mut files = Vec::new();
        let mut bad_lines = Vec::new();
        for entry in fs::read_dir(&self.args.out_dir)? {
            let entry = entry?;
            let path = entry.path();
            let is_log = path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("server-") && name.ends_with(".log"));
            if !is_log {
                continue;
            }
            let text = fs::read_to_string(&path).unwrap_or_default();
            let mut line_count = 0usize;
            for (idx, line) in text.lines().enumerate() {
                line_count += 1;
                let lower = line.to_ascii_lowercase();
                let bad = lower.contains(" warn ")
                    || lower.contains(" error ")
                    || lower.contains("panic")
                    || lower.contains("critical")
                    || lower.contains("degraded")
                    || lower.contains("failed");
                if bad {
                    bad_lines.push(json!({
                        "file": path,
                        "line": idx + 1,
                        "text": line,
                    }));
                }
            }
            files.push(json!({
                "file": path,
                "lines": line_count,
            }));
        }
        if !bad_lines.is_empty() {
            return Err(format!("server logs contain warnings/errors: {bad_lines:?}").into());
        }
        Ok(json!({
            "status": "PASS",
            "files": files,
            "bad_lines": bad_lines,
        }))
    }

    fn write_summary(&self) -> ProofResult<()> {
        fs::write(
            self.args.out_dir.join("summary.json"),
            serde_json::to_vec_pretty(&self.summary)?,
        )?;
        Ok(())
    }

    fn cleanup_roots(&self) -> ProofResult<()> {
        for root in self.index_roots.iter().chain(self.blob_roots.iter()) {
            if let Some(parent) = root.parent() {
                let _ = fs::remove_dir_all(parent);
            }
        }
        Ok(())
    }
}

impl Drop for Proof {
    fn drop(&mut self) {
        let _ = self.stop_server();
    }
}

fn encode_path(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC)
        .to_string()
        .replace("%2F", "/")
}

fn encode_query_value(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

fn split_parent_name(path: &str) -> (&str, &str) {
    match path.rsplit_once('/') {
        Some((parent, name)) => (parent, name),
        None => ("", path),
    }
}

fn collect_unhealthy_strings(path: &str, value: &Value, bad: &mut Vec<(String, String)>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                let next = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                collect_unhealthy_strings(&next, child, bad);
            }
        }
        Value::Array(items) => {
            for (idx, child) in items.iter().enumerate() {
                collect_unhealthy_strings(&format!("{path}[{idx}]"), child, bad);
            }
        }
        Value::String(text) => {
            let key = path.rsplit('.').next().unwrap_or(path).to_ascii_lowercase();
            let status_like = key.contains("status")
                || key.contains("state")
                || key.contains("health")
                || key.contains("severity")
                || key == "phase"
                || key == "level";
            let normalized = text.to_ascii_lowercase().replace(' ', "_");
            if status_like
                && matches!(
                    normalized.as_str(),
                    "critical" | "critical_failure" | "failed" | "failure" | "degraded"
                )
            {
                bad.push((path.to_string(), text.clone()));
            }
        }
        _ => {}
    }
}

fn count_files_with_suffix(root: &Path, suffix: &str) -> ProofResult<usize> {
    if !root.exists() {
        return Ok(0);
    }
    let mut count = 0;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            let ty = entry.file_type()?;
            if ty.is_dir() {
                stack.push(path);
            } else if ty.is_file()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with(suffix))
            {
                count += 1;
            }
        }
    }
    Ok(count)
}

fn count_symlinks_excluding(root: &Path, excluded_segment: &str) -> ProofResult<usize> {
    if !root.exists() {
        return Ok(0);
    }
    let mut count = 0;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            let ty = fs::symlink_metadata(&path)?.file_type();
            let rel = path.strip_prefix(root).unwrap_or(&path);
            let excluded = rel
                .components()
                .any(|part| part.as_os_str() == excluded_segment);
            if excluded {
                continue;
            }
            if ty.is_symlink() {
                count += 1;
            } else if ty.is_dir() {
                stack.push(path);
            }
        }
    }
    Ok(count)
}

fn count_object_record_dirs(root: &Path) -> ProofResult<usize> {
    if !root.exists() {
        return Ok(0);
    }
    let mut count = 0;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("");
            if name.starts_with("@o_") {
                if count_files_with_suffix(&path, ".ndx")? == 0 {
                    return Err(format!(
                        "object record directory has no committed .ndx: {}",
                        path.display()
                    )
                    .into());
                }
                count += 1;
            } else {
                stack.push(path);
            }
        }
    }
    Ok(count)
}

fn count_dirs(root: &Path) -> ProofResult<usize> {
    if !root.exists() {
        return Ok(0);
    }
    let mut count = 0;
    for entry in fs::read_dir(root)? {
        if entry?.file_type()?.is_dir() {
            count += 1;
        }
    }
    Ok(count)
}

fn deterministic_payload(size: usize, seed: &[u8]) -> Vec<u8> {
    let digest = Sha256::digest(seed);
    let mut out = Vec::with_capacity(size);
    while out.len() < size {
        out.extend_from_slice(&digest);
    }
    out.truncate(size);
    out
}

fn sha256_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

async fn response_bytes_limited(
    response: reqwest::Response,
    max_bytes: usize,
    label: &str,
) -> ProofResult<Vec<u8>> {
    let mut out = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let next_len = out.len().saturating_add(chunk.len());
        if next_len > max_bytes {
            return Err(format!(
                "{label} response exceeded proof memory cap: {} > {} bytes",
                next_len, max_bytes
            )
            .into());
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

fn tar_names(raw: &[u8]) -> ProofResult<BTreeSet<String>> {
    let mut names = BTreeSet::new();
    let mut offset = 0usize;
    while offset + 512 <= raw.len() {
        let header = &raw[offset..offset + 512];
        if header.iter().all(|byte| *byte == 0) {
            break;
        }
        let name = parse_tar_string(&header[0..100]);
        let prefix = parse_tar_string(&header[345..500]);
        let full_name = if prefix.is_empty() {
            name
        } else {
            format!("{prefix}/{name}")
        };
        if !full_name.is_empty() {
            names.insert(full_name);
        }
        let size_text = parse_tar_string(&header[124..136]);
        let size = usize::from_str_radix(size_text.trim(), 8).unwrap_or(0);
        let blocks = size.div_ceil(512);
        offset += 512 + blocks * 512;
    }
    Ok(names)
}

fn parse_tar_string(bytes: &[u8]) -> String {
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).trim().to_string()
}

fn read_rss_kib(pid: u32) -> ProofResult<Option<u64>> {
    let status = fs::read_to_string(format!("/proc/{pid}/status"))?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let value = rest
                .split_whitespace()
                .next()
                .and_then(|part| part.parse::<u64>().ok());
            return Ok(value);
        }
    }
    Ok(None)
}

fn round3(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let mut proof = match Proof::new(args) {
        Ok(proof) => proof,
        Err(err) => {
            eprintln!("N1_RELEASE_PROOF FAIL {err}");
            std::process::exit(1);
        }
    };
    match proof.run().await {
        Ok(()) => {
            println!("N1_RELEASE_PROOF PASS");
        }
        Err(err) => {
            proof.summary = json!({
                "status": "FAIL",
                "error": err.to_string(),
            });
            let _ = proof.write_summary();
            eprintln!("N1_RELEASE_PROOF FAIL {err}");
            std::process::exit(1);
        }
    }
}
