impl Proof {
    fn validate(&mut self) -> ProofResult<()> {
        if !self.args.binary.is_file() {
            return Err(format!("N1 binary does not exist: {}", self.args.binary.display()).into());
        }
        self.args.binary = fs::canonicalize(&self.args.binary)?;
        if self.args.spare_root.is_some() && !self.args.pool_maintenance_proof && !self.args.pool_relocation_proof && !self.args.backup_proof {
            return Err("spare-root requires a pool-maintenance or backup proof".into());
        }
        if !self.args.source_file.is_file() {
            return Err(format!(
                "source file does not exist: {}",
                self.args.source_file.display()
            )
            .into());
        }
        self.args.source_file = fs::canonicalize(&self.args.source_file)?;
        if !self.args.prefix.ends_with('/') {
            self.args.prefix.push('/');
        }
        let roots = self.disk_roots()?;
        self.active_disk_count = roots.len();
        let focused_single_root_functional = (self.args.noa_api_proof || self.args.export_proof || self.args.pool_maintenance_proof || self.args.pool_relocation_proof || self.args.backup_proof || self.args.online_backup_proof)
            && self.data_shards == 1
            && self.parity_shards == 0
            && roots.len() == 1;
        if roots.len() < 2 && !focused_single_root_functional {
            return Err(
                "release proof requires at least two roots; only focused NOA API and export proofs permit an explicit 1+0 functional topology"
                    .into(),
            );
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
        if self.args.gc_workload_timeout_seconds == 0 {
            return Err("gc_workload_timeout_seconds must be at least 1".into());
        }
        if self.args.trash_retention_seconds < 5 {
            return Err(
                "trash_retention_seconds must be at least 5 for deterministic recovery proofs"
                    .into(),
            );
        }
        if self.args.export_proof {
            if self.args.export_objects == 0 {
                return Err("export_objects must be at least 1".into());
            }
            if self.args.export_object_bytes == 0 {
                return Err("export_object_bytes must be at least 1".into());
            }
            if self.args.export_upload_concurrency == 0 {
                return Err("export_upload_concurrency must be at least 1".into());
            }
            if self.args.export_retention_seconds < 60 {
                return Err("export_retention_seconds must be at least 60".into());
            }
        }
        for root in &roots {
            if !root.is_dir() {
                return Err(format!("disk root does not exist: {}", root.display()).into());
            }
        }
        if self.data_shards == 0 {
            return Err("data_shards must be at least 1".into());
        }
        let total_shards = usize::from(self.data_shards) + usize::from(self.parity_shards);
        if total_shards == 0 || total_shards > roots.len() {
            return Err(format!(
                "data_shards + parity_shards must fit active disk roots: data={} parity={} roots={}",
                self.data_shards,
                self.parity_shards,
                roots.len()
            )
            .into());
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
            let index = root.join("store");
            let blob = index.clone();
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
                .args(["-subj", "/CN=127.0.0.1", "-addext", "subjectAltName=IP:127.0.0.1"])
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
        let blob_roots = self
            .blob_roots
            .iter()
            .take(self.active_disk_count)
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(",");
        let auto_healing_enabled = self.args.auto_healing_proof
            || self.args.trash_maintenance_proof
            || self.args.maintenance_coverage_proof;
        let topology_recovery_stability_seconds = if auto_healing_enabled { 2 } else { 0 };
        let initial_scan_delay_seconds = if self.args.maintenance_coverage_proof {
            3600
        } else if auto_healing_enabled {
            2
        } else {
            60
        };
        let availability_scan_interval_seconds = if self.args.maintenance_coverage_proof {
            3600
        } else if auto_healing_enabled {
            2
        } else {
            3600
        };
        let checksum_scrub_interval_seconds = if self.args.maintenance_coverage_proof {
            3600
        } else if self.args.trash_maintenance_proof {
            300
        } else if auto_healing_enabled {
            5
        } else {
            604800
        };
        let config = format!(
            r#"[server]
host={}
https_port={}
native_port={}
cert_path={}
key_path={}

[disk_config]
fs_storage_path={}

[storage_pool.A]
generation=1
roots={}
data_shards={}
parity_shards={}
sealed=false

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
trash_retention_seconds={}

[export]
retention_seconds={}
cleanup_interval_seconds=5
max_concurrent_jobs=2
max_queued_jobs=32
manifest_validation_concurrency=8
object_read_concurrency=8
artifact_chunk_size_bytes={export_chunk_size}
artifact_write_concurrency=4
max_buffered_bytes_per_job=67108864
max_buffered_bytes_global=134217728
max_concurrent_downloads=16
max_retained_bytes=0

[auto_healing]
enabled={}
topology_check_interval_seconds=1
topology_recovery_stability_seconds={}
initial_scan_delay_seconds={}
availability_scan_interval_seconds={}
checksum_scrub_interval_seconds={}
object_scan_concurrency=1
shard_repair_concurrency=1
metadata_batch_size=128

[set_limits]
max_file_size=1099511627776

[jwt_token_config]
login_token_expiration=900

[rate_limiter_config]
enable_rate_limiting={}
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
stripe_block_size=262144
"#,
            self.args.listen_host,
            self.args.port,
            self.native_port(),
            cert.display(),
            key.display(),
            self.args.out_dir.join("fs").display(),
            blob_roots,
            self.data_shards,
            self.parity_shards,
            self.args.trash_retention_seconds,
            self.args.export_retention_seconds,
            auto_healing_enabled,
            topology_recovery_stability_seconds,
            initial_scan_delay_seconds,
            availability_scan_interval_seconds,
            checksum_scrub_interval_seconds,
            self.args.rate_limiter_enabled,
            self.args.max_part_size,
            export_chunk_size = PROOF_EXPORT_CHUNK_SIZE,
        );
        fs::write(self.args.out_dir.join("n1_config.ini"), config)?;
        Ok(())
    }

    async fn start_server(&mut self, label: &str) -> ProofResult<()> {
        let log = fs::File::create(self.args.out_dir.join(format!("server-{label}.log")))?;
        let err = log.try_clone()?;
        let mut command = Command::new(&self.args.binary);
        if self.args.out_dir.join("live-operator.json").is_file() {
            command.env("N1_BACKUP_OPERATOR_CONFIG", self.args.out_dir.join("live-operator.json"));
        } else { command.env_remove("N1_BACKUP_OPERATOR_CONFIG"); }
        let child = command.current_dir(&self.args.out_dir)
            .env("RUST_LOG", "info")
            .env("RUST_BACKTRACE", "1")
            .env("N1_REFRESH_STORE_ROOT", self.args.out_dir.join("refresh"))
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(err))
            .spawn()?;
        self.server = Some(child);
        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            if let Some(child) = self.server.as_mut()
                && let Some(status) = child.try_wait()?
            {
                return Err(format!("N1 exited during startup with {status}").into());
            }
            if let Ok(response) = self
                .client
                .get(format!("{}/noa/v1/ping", self.base_url))
                .send()
                .await
                && response.status() == StatusCode::OK
                && response.bytes().await?.as_ref() == b"pong"
            {
                return Ok(());
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
        });
        let response = self
            .client
            .post(format!("{}/noa/v1/fragments", self.base_url))
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
        self.stable_fragment_id = self.resolve_stable_fragment_id()?;
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
            .post(format!("{}/noa/v1/auth/fragment", self.base_url))
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
        // Normal workloads keep fragment authority; only server-wide controls
        // use the separately authenticated master-key operator capability.
        self.operator_token = self.auth_fragment(&self.fragment, &self.args.master_key).await?;
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
            .post(format!("{}/noa/v1/auth/fragment", self.base_url))
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
