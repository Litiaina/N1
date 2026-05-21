impl Proof {
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
native_port={}
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
upload_io_gate_enabled=true
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
            self.native_port(),
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
}
