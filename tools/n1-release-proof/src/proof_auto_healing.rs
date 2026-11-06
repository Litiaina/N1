struct DisplacedProofRoot {
    original: PathBuf,
    displaced: PathBuf,
    restored: bool,
}

impl DisplacedProofRoot {
    fn new(original: &Path, suffix: &str) -> ProofResult<Self> {
        let parent = original
            .parent()
            .ok_or_else(|| format!("proof root has no parent: {}", original.display()))?;
        let file_name = original
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| format!("proof root has no valid name: {}", original.display()))?;
        let displaced = parent.join(format!("{file_name}.{suffix}"));
        if displaced.exists() {
            fs::remove_dir_all(&displaced)?;
        }
        fs::rename(original, &displaced)?;
        Ok(Self {
            original: original.to_path_buf(),
            displaced,
            restored: false,
        })
    }

    fn install_empty_replacement(&self) -> ProofResult<()> {
        fs::create_dir_all(&self.original)?;
        Ok(())
    }

    fn restore(mut self) -> ProofResult<()> {
        if self.original.exists() {
            fs::remove_dir_all(&self.original)?;
        }
        fs::rename(&self.displaced, &self.original)?;
        self.restored = true;
        Ok(())
    }
}

impl Drop for DisplacedProofRoot {
    fn drop(&mut self) {
        if self.restored {
            return;
        }
        if self.original.exists() {
            let _ = fs::remove_dir_all(&self.original);
        }
        let _ = fs::rename(&self.displaced, &self.original);
    }
}

impl Proof {
    async fn run_trash_maintenance_proof(&mut self) -> ProofResult<()> {
        const DIRECTORY_OBJECTS: usize = 140;

        self.validate()?;
        if self.data_shards == 0 || self.parity_shards == 0 {
            return Err("trash maintenance proof requires non-zero data and parity shards".into());
        }
        self.prepare_dirs()?;
        self.write_config()?;
        self.start_server("trash-maintenance-before-restart")
            .await?;
        self.wait_for_log_count(
            "trash-maintenance-before-restart",
            "leaving automatic read-only mode",
            1,
            Duration::from_secs(30),
        )
        .await?;
        self.create_fragment().await?;
        self.auth().await?;
        self.wait_for_write_safe("trash maintenance startup")
            .await?;

        let base = format!("{}trash-maintenance-directory", self.args.prefix);
        let nested = format!("{base}/nested");
        // N1's flat namespace has no empty directory objects. Uploading the
        // keys below establishes these prefixes; mkdir is correctly rejected.

        let direct_key = format!("{}trash-maintenance-direct.bin", self.args.prefix);
        let direct_payload = deterministic_payload(8 * 1024, b"n1-trash-direct");
        let direct_sha = sha256_bytes(&direct_payload);
        let direct_version = self
            .upload_bytes(&direct_key, direct_payload, "application/octet-stream")
            .await?;

        let mut directory_objects = Vec::with_capacity(DIRECTORY_OBJECTS);
        for index in 0..DIRECTORY_OBJECTS {
            let key = format!("{nested}/item-{index:03}.bin");
            let seed = format!("n1-trash-directory-{index:03}");
            let payload = deterministic_payload(4 * 1024, seed.as_bytes());
            let sha = sha256_bytes(&payload);
            let version = self
                .upload_bytes(&key, payload, "application/octet-stream")
                .await?;
            directory_objects.push((key, sha, version));
        }

        self.posix_post("/noa/v1/posix/trash", json!({"path": &direct_key}))
            .await?;
        self.posix_post("/noa/v1/posix/trash", json!({"path": &base}))
            .await?;

        let removed_direct = self.remove_one_shard_matching(&direct_version, |path| {
            path.extension().and_then(|ext| ext.to_str()) == Some("parity")
        })?;
        let last_version = &directory_objects
            .last()
            .ok_or("trash maintenance proof created no directory objects")?
            .2;
        let removed_after_page_boundary = self.remove_one_shard_matching(last_version, |path| {
            path.extension().and_then(|ext| ext.to_str()) == Some("parity")
        })?;

        self.stop_server()?;
        self.start_server("trash-maintenance-after-restart").await?;
        self.auth().await?;
        self.wait_for_write_safe("trash restart topology validation").await?;
        self.explicit_repair_and_wait("availability").await?;
        self.wait_for_path(&removed_direct, true, Duration::from_secs(30))
            .await?;
        self.wait_for_path(&removed_after_page_boundary, true, Duration::from_secs(30))
            .await?;
        let scan_health = self.auto_healing_health().await?;

        self.posix_post("/noa/v1/posix/recover", json!({"path": &direct_key}))
            .await?;
        self.posix_post("/noa/v1/posix/recover", json!({"path": &base}))
            .await?;
        self.assert_download_sha256(&direct_key, &direct_sha)
            .await?;
        for (key, sha, _) in &directory_objects {
            self.assert_download_sha256(key, sha).await?;
        }
        let final_health = self
            .wait_for_write_safe("post-recovery write safety")
            .await?;

        self.summary = json!({
            "status": "PASS",
            "proof": "n1_trash_maintenance_real_disk",
            "physical_topology": {
                "disk_roots": self.disk_roots()?,
                "data_shards": self.data_shards,
                "parity_shards": self.parity_shards,
                "colocated_roots": self.args.colocated_roots,
            },
            "trash_scope": {
                "direct_objects": 1,
                "deleted_directory_objects": DIRECTORY_OBJECTS,
                "metadata_batch_size": 128,
                "crossed_batch_boundary": DIRECTORY_OBJECTS > 128,
            },
            "fault_injection": {
                "direct_trash_shard_removed_and_restored": removed_direct,
                "directory_tail_shard_removed_and_restored": removed_after_page_boundary,
                "server_restarted_while_objects_were_trashed": true,
            },
            "recovery_validation": {
                "objects_downloaded_and_sha256_verified": DIRECTORY_OBJECTS + 1,
            },
            "scan_health": scan_health,
            "final_health": final_health,
        });
        self.write_summary()?;
        self.stop_server()?;
        if self.args.cleanup_roots {
            self.cleanup_roots()?;
        }
        Ok(())
    }

    async fn auto_healing_health(&self) -> ProofResult<Value> {
        let response = self
            .client
            .get(format!(
                "{}/noa/v1/maintenance/health?refresh=true",
                self.base_url
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "auto-healing health failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(response.json().await?)
    }

    async fn wait_for_auto_healing_state<F>(
        &self,
        label: &str,
        timeout: Duration,
        predicate: F,
    ) -> ProofResult<Value>
    where
        F: Fn(&Value) -> bool,
    {
        let deadline = Instant::now() + timeout;
        let mut last = json!(null);
        while Instant::now() < deadline {
            last = self.auto_healing_health().await?;
            if predicate(&last) {
                return Ok(last);
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        Err(format!("timed out waiting for {label}; last={last}").into())
    }

    async fn wait_for_write_safe(&self, label: &str) -> ProofResult<Value> {
        self.wait_for_auto_healing_state(label, Duration::from_secs(30), |snapshot| {
            snapshot
                .pointer("/maintenance/auto_healing/write_safe")
                .and_then(Value::as_bool)
                == Some(true)
                && snapshot
                    .pointer("/maintenance/auto_healing/read_only_reason")
                    .is_none_or(Value::is_null)
                && snapshot
                    .pointer("/maintenance/access_mode")
                    .and_then(Value::as_str)
                    == Some("availability")
        })
        .await
    }

    fn version_shard_paths(&self, version_id: &str) -> ProofResult<Vec<PathBuf>> {
        let mut paths = Vec::new();
        for version_dir in self.object_version_dirs(version_id)? {
            paths.extend(
                fs::read_dir(version_dir)?
                    .filter_map(Result::ok)
                    .map(|entry| entry.path())
                    .filter(|path| path.is_file() && is_erasure_shard_path(path)),
            );
        }
        paths.sort();
        paths.dedup();
        Ok(paths)
    }

    async fn assert_mutation_service_unavailable(&self, object_key: &str) -> ProofResult<Value> {
        let response = self
            .client
            .post(format!(
                "{}/noa/v1/upload/one-shot/{}",
                self.base_url,
                encode_path(object_key)
            ))
            .bearer_auth(&self.token)
            .header("content-type", "text/plain")
            .body("must be rejected while topology is unsafe")
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if status != StatusCode::SERVICE_UNAVAILABLE {
            return Err(format!(
                "unsafe-topology mutation was not rejected with 503: status={status} body={body}"
            )
            .into());
        }
        Ok(json!({"status": status.as_u16(), "body": body}))
    }

    async fn explicit_repair_and_wait(&self, mode: &str) -> ProofResult<Value> {
        self.start_manual_repair_mode(mode).await?;
        let result = self
            .wait_job("/noa/v1/maintenance/repair/status", mode)
            .await?;
        if result
            .get("failed_repairs")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            != 0
        {
            return Err(format!("explicit repair left failed objects: {result}").into());
        }
        Ok(result)
    }

    async fn prove_read_bitrot_paths(&self) -> ProofResult<Value> {
        let mut results = Vec::new();
        for endpoint in [
            "get",
            "stream",
            "download",
            "range",
            "chunk",
            "native-full",
            "native-stream",
            "native-chunk",
            "archive",
        ] {
            let key = format!("{}integrity/{endpoint}.bin", self.args.prefix);
            let payload = deterministic_payload(65_537, endpoint.as_bytes());
            let expected_sha = sha256_bytes(&payload);
            let version = self
                .upload_bytes(&key, payload.clone(), "application/octet-stream")
                .await?;
            self.assert_download_sha256(&key, &expected_sha).await?;
            let paths = self.version_shard_paths(&version)?;
            let path = paths
                .iter()
                .find(|path| path.extension().and_then(|value| value.to_str()) == Some("shard"))
                .ok_or("no actual data shard to corrupt")?;
            let raw = fs::read(path)?;
            let raw_sha = sha256_bytes(&raw);
            let mut metadata_before = Vec::new();
            for shard in &paths {
                for entry in fs::read_dir(
                    shard
                        .parent()
                        .and_then(Path::parent)
                        .ok_or("missing object parent")?,
                )? {
                    let entry = entry?;
                    if entry.file_type()?.is_file()
                        && entry.path().extension().and_then(|value| value.to_str()) == Some("ndx")
                    {
                        metadata_before
                            .push((entry.path(), sha256_bytes(&fs::read(entry.path())?)));
                    }
                }
            }
            if metadata_before.is_empty() {
                return Err("no NDX replicas to check immutability".into());
            }
            let mut damaged = raw.clone();
            damaged[17] ^= 0x80;
            fs::write(path, &damaged)?;
            let started = Instant::now();
            let mut expected = payload.clone();
            if endpoint.starts_with("native-") {
                let mut connection =
                    TcpStream::connect(("127.0.0.1", self.native_port())).await?;
                let auth = native_round_trip(&mut connection, "integrity-auth", "auth.fragment",
                    json!({"fragment":self.fragment,"secret":self.args.fragment_key,"read":true,"expires_in":21600}), None).await?;
                ensure_native_ok(&auth, "integrity native auth")?;
                if endpoint == "native-stream" {
                    let result = native_round_trip_stream_hash(
                        &mut connection,
                        "integrity-stream",
                        "object.download_stream",
                        json!({"object_key":key,"version_id":version,"chunk_size":4096}),
                    )
                    .await?;
                    ensure_native_status(&result.response, 200, "verified native stream")?;
                    if result.bytes_read != payload.len() || result.sha256 != expected_sha {
                        return Err(
                            format!("native stream returned corrupted data for {key}").into()
                        );
                    }
                } else {
                    let (op, request) = if endpoint == "native-chunk" {
                        expected = payload[3..1003].to_vec();
                        (
                            "object.download_chunk",
                            json!({"object_key":key,"version_id":version,"offset":3,"chunk_size":1000}),
                        )
                    } else {
                        (
                            "object.download",
                            json!({"object_key":key,"version_id":version}),
                        )
                    };
                    let result =
                        native_round_trip(&mut connection, "integrity-read", op, request, None)
                            .await?;
                    ensure_native_status(&result, 200, "verified native read")?;
                    if result.body.as_ref().map(|body| body.as_slice()) != Some(expected.as_slice())
                    {
                        return Err(format!("native read returned corrupted data for {key}").into());
                    }
                }
            } else {
                let mut request = match endpoint {
                    "get" => self.client.get(format!("{}/noa/v1/objects/get/{}", self.base_url, encode_path(&key))),
                    "stream" => self.client.get(format!("{}/noa/v1/objects/stream/{}", self.base_url, encode_path(&key))),
                    "chunk" => self.client.get(format!("{}/noa/v1/objects/download/chunk", self.base_url))
                        .query(&[("object_key",key.clone()),("version_id",version.clone()),("offset","3".into()),("chunk_size","1000".into())]),
                    "archive" => self.client.post(format!("{}/noa/v1/objects/archive",self.base_url))
                        .json(&json!({"format":"tar","keys":[key]})),
                    _ => self.client.get(format!("{}/noa/v1/objects/download",self.base_url))
                        .query(&[("object_key",key.clone()),("version_id",version.clone())]),
                };
                if endpoint == "range" {
                    request = request.header("Range", "bytes=3-1002");
                    expected = payload[3..1003].to_vec();
                } else if endpoint == "chunk" {
                    expected = payload[3..1003].to_vec();
                }
                let response = request.bearer_auth(&self.token).send().await?;
                let expected_status = if endpoint == "range" {
                    StatusCode::PARTIAL_CONTENT
                } else {
                    StatusCode::OK
                };
                if response.status() != expected_status {
                    return Err(format!("{endpoint} bitrot read failed: {} {}", response.status(), response.text().await?).into());
                }
                let bytes = response.bytes().await?;
                if endpoint == "archive" {
                    if tar_payload_hashes(&bytes)?.get(&key) != Some(&expected_sha) {
                        return Err("archive exposed corrupt payload".into());
                    }
                } else if bytes.as_ref() != expected.as_slice() {
                    return Err(format!("{endpoint} exposed corrupt payload").into());
                }
            }
            let read_ms = started.elapsed().as_secs_f64() * 1000.0;
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                if fs::read(path).is_ok_and(|bytes| sha256_bytes(&bytes) == raw_sha) {
                    break;
                }
                if Instant::now() >= deadline {
                    return Err(format!(
                        "{endpoint}: bytes recovered but physical shard not repaired"
                    )
                    .into());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            for (metadata, before) in metadata_before {
                if sha256_bytes(&fs::read(metadata)?) != before {
                    return Err("read repair rewrote committed NDX".into());
                }
            }
            results.push(
                json!({
                    "endpoint": endpoint,
                    "same_length_bitrot": true,
                    "returned_bytes_verified": true,
                    "physical_shard_repaired": true,
                    "ndx_unchanged": true,
                    "read_ms": read_ms
                }),
            );
        }
        Ok(json!({"paths":results}))
    }

    async fn prove_concurrent_bitrot_reads(&self) -> ProofResult<Value> {
        const OBJECTS: usize = 16;
        const READERS: usize = 64;
        let mut fixtures = Vec::new();
        for index in 0..OBJECTS {
            let key = format!("{}integrity/burst-{index:03}.bin", self.args.prefix);
            let payload = deterministic_payload(65_537, key.as_bytes());
            let digest = sha256_bytes(&payload);
            let version = self.upload_bytes(&key, payload, "application/octet-stream").await?;
            let path = self.version_shard_paths(&version)?.into_iter()
                .find(|path| path.extension().and_then(|ext| ext.to_str()) == Some("shard"))
                .ok_or("burst fixture has no data shard")?;
            let original = fs::read(&path)?;
            let raw_digest = sha256_bytes(&original);
            let mut damaged = original;
            damaged[17] ^= 0x80;
            fs::write(&path, damaged)?;
            fixtures.push((key, version, digest, path, raw_digest));
        }
        let started = Instant::now();
        // Poll all futures together: distinct corrupt targets and duplicate
        // readers compete for the same bounded repair admission.
        let reads = (0..READERS).map(|index| {
            let (key, version, digest, _, _) = &fixtures[index % OBJECTS];
            async move {
                let response = self.client.get(format!("{}/noa/v1/objects/download", self.base_url))
                    .bearer_auth(&self.token)
                    .query(&[("object_key", key), ("version_id", version)])
                    .send().await?.error_for_status()?;
                let bytes = response.bytes().await?;
                if bytes.len() != 65_537 || sha256_bytes(&bytes) != *digest {
                    return Err(format!("concurrent bitrot read returned bad bytes: {key}").into());
                }
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            }
        });
        for read in join_all(reads).await {
            read?;
        }
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            if fixtures.iter().all(|(_, _, _, path, digest)| {
                fs::read(path).is_ok_and(|bytes| sha256_bytes(&bytes) == *digest)
            }) {
                break;
            }
            if Instant::now() >= deadline {
                return Err("concurrent reads returned but not all corrupt shards were physically repaired".into());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(json!({
            "corrupt_objects": OBJECTS,
            "concurrent_readers": READERS,
            "all_responses_sha256_verified": true,
            "all_shards_physically_repaired": true,
            "elapsed_seconds": started.elapsed().as_secs_f64()
        }))
    }

    async fn prove_explicit_checksum_gate(&self) -> ProofResult<Value> {
        self.start_manual_repair_mode("checksum").await?;
        let deadline = Instant::now() + Duration::from_secs(45);
        while Instant::now() < deadline {
            let health = self.auto_healing_health().await?;
            let running = health
                .pointer("/maintenance/repair_running")
                .and_then(Value::as_bool)
                == Some(true);
            if running {
                let key = format!(
                    "{}integrity/concurrent-checksum-write.bin",
                    self.args.prefix
                );
                let (status, body) = self.raw_one_shot_mutation(&key).await?;
                if status != StatusCode::FORBIDDEN {
                    return Err(format!(
                        "explicit checksum must preserve its read-only gate: {status} {body}"
                    )
                    .into());
                }
                return Ok(json!({"running_health":health,"write_status":status.as_u16()}));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Err("explicit checksum was not observed running".into())
    }

    async fn run_auto_healing_proof(&mut self) -> ProofResult<()> {
        self.validate()?;
        if self.data_shards == 0 || self.parity_shards == 0 {
            return Err("auto-healing proof requires non-zero data and parity shards".into());
        }
        let original_data_shards = self.data_shards;
        let original_parity_shards = self.parity_shards;
        let original_width =
            usize::from(original_data_shards) + usize::from(original_parity_shards);
        self.prepare_dirs()?;
        self.write_config()?;
        self.start_server("auto-healing").await?;
        self.wait_for_log_count(
            "auto-healing",
            "leaving automatic read-only mode",
            1,
            Duration::from_secs(30),
        )
        .await?;
        self.create_fragment().await?;
        self.auth().await?;

        let startup = self
            .wait_for_write_safe("startup topology validation")
            .await?;
        let read_integrity = self.prove_read_bitrot_paths().await?;
        let concurrent_read_integrity = self.prove_concurrent_bitrot_reads().await?;
        fs::write(self.args.out_dir.join("read-integrity.json"), serde_json::to_vec_pretty(&json!({
            "status": "PASS",
            "scope": "read paths only; not the complete fault matrix or scale qualification",
            "read_integrity":&read_integrity,
            "concurrent_read_integrity":&concurrent_read_integrity,
        }))?)?;
        let object_key = format!("{}auto-healing/source.bin", self.args.prefix);
        let payload = deterministic_payload(2 * 1024 * 1024, b"n1-auto-healing-proof");
        let payload_sha = sha256_bytes(&payload);
        let version_id = self
            .upload_bytes(&object_key, payload, "application/octet-stream")
            .await?;
        let initial_shards = self.version_shard_paths(&version_id)?;
        if initial_shards.len() != original_width {
            return Err(format!(
                "{}+{} object did not create exactly {} shards: {initial_shards:?}",
                original_data_shards, original_parity_shards, original_width
            )
            .into());
        }

        let removed_on_read = self.remove_one_data_shard(&version_id)?;
        self.assert_download_sha256(&object_key, &payload_sha)
            .await?;
        self.wait_for_path(&removed_on_read, true, Duration::from_secs(30))
            .await?;
        self.assert_object_status_clean(&object_key, "recover_on_read")
            .await?;

        let removed_by_scan = self.remove_one_shard_matching(&version_id, |path| {
            path.extension().and_then(|ext| ext.to_str()) == Some("parity")
        })?;
        tokio::time::sleep(Duration::from_secs(3)).await;
        if removed_by_scan.exists() {
            return Err("cold parity was repaired by an unsolicited scan".into());
        }
        let quiet = self.auto_healing_health().await?;
        if quiet
            .pointer("/maintenance/auto_healing/last_availability_scan_at")
            .is_some_and(|value| !value.is_null())
            || quiet
                .pointer("/maintenance/auto_healing/last_checksum_scrub_at")
                .is_some_and(|value| !value.is_null())
        {
            return Err("periodic full scan ran without an operator request".into());
        }
        let scheduled_repair = self.explicit_repair_and_wait("availability").await?;
        self.wait_for_path(&removed_by_scan, true, Duration::from_secs(30))
            .await?;
        self.assert_object_status_clean(&object_key, "explicit_repair")
            .await?;

        let checksum_key = format!("{}auto-healing/checksum-baseline.bin", self.args.prefix);
        let checksum_payload =
            deterministic_payload(128 * 1024 * 1024, b"n1-online-checksum-proof");
        let checksum_sha = sha256_bytes(&checksum_payload);
        self.upload_complete_multipart_payload(&checksum_key, &checksum_payload, 32 * 1024 * 1024)
            .await?;
        let explicit_checksum_gate = self.prove_explicit_checksum_gate().await?;
        let checksum_result = self
            .wait_job("/noa/v1/maintenance/repair/status", "explicit checksum")
            .await?;
        if checksum_result
            .get("failed_repairs")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            != 0
        {
            return Err(format!("checksum repair left failures: {checksum_result}").into());
        }
        self.assert_download_sha256(&checksum_key, &checksum_sha)
            .await?;

        let root = self
            .index_roots
            .first()
            .cloned()
            .ok_or("auto-healing proof has no first root")?;
        let missing = DisplacedProofRoot::new(&root, "missing")?;
        let missing_health = self
            .wait_for_auto_healing_state(
                "missing-root read-only gate",
                Duration::from_secs(30),
                |snapshot| {
                    snapshot
                        .pointer("/maintenance/auto_healing/write_safe")
                        .and_then(Value::as_bool)
                        == Some(false)
                        && snapshot
                            .pointer("/maintenance/access_mode")
                            .and_then(Value::as_str)
                            == Some("read_only")
                },
            )
            .await?;
        self.assert_download_sha256(&object_key, &payload_sha)
            .await?;
        let rejected_mutation = self
            .assert_mutation_service_unavailable(&format!(
                "{}auto-healing/rejected.bin",
                self.args.prefix
            ))
            .await?;
        missing.restore()?;
        let restored_health = self.wait_for_write_safe("restored-root recovery").await?;
        self.assert_download_sha256(&object_key, &payload_sha)
            .await?;

        let replacement = DisplacedProofRoot::new(&root, "identity-mismatch")?;
        replacement.install_empty_replacement()?;
        let replacement_health = self
            .wait_for_auto_healing_state(
                "replacement identity rejection",
                Duration::from_secs(30),
                |snapshot| {
                    snapshot
                        .pointer("/maintenance/auto_healing/write_safe")
                        .and_then(Value::as_bool)
                        == Some(false)
                        && snapshot
                            .get("disks")
                            .and_then(Value::as_array)
                            .is_some_and(|disks| {
                                disks.iter().any(|disk| {
                                    disk.get("error")
                                        .and_then(Value::as_str)
                                        .is_some_and(|error| {
                                            error.contains(
                                                "does not match its registered N1 root identity",
                                            )
                                        })
                                })
                            })
                },
            )
            .await?;
        replacement.restore()?;
        let identity_restored_health = self
            .wait_for_write_safe("identity-authoritative root restoration")
            .await?;

        let shard_paths = self.version_shard_paths(&version_id)?;
        let backup_dir = self.args.out_dir.join("unrecoverable-shard-backup");
        fs::create_dir_all(&backup_dir)?;
        let mut backups = Vec::new();
        for (index, path) in shard_paths.iter().enumerate() {
            let backup = backup_dir.join(format!("shard-{index}"));
            fs::copy(path, &backup)?;
            backups.push((path.clone(), backup));
        }
        // A live healer can reconstruct a removed shard while another large
        // backup is copied, or retain a valid open descriptor. Close this owned
        // engine before injecting the complete cold beyond-parity fault.
        // Recovery below restores only k pieces while the engine runs.
        self.stop_server()?;
        for (path, _) in &backups {
            fs::remove_file(path)?;
        }
        self.start_server("auto-healing-unrecoverable").await?;
        self.wait_for_write_safe("root identity validation after cold damage")
            .await?;
        let response = self
            .client
            .get(format!("{}/noa/v1/objects/download", self.base_url))
            .query(&[("object_key", object_key.clone())])
            .bearer_auth(&self.token)
            .send()
            .await?;
        let status = response.status();
        if status.is_success() && response.bytes().await.is_ok() {
            return Err("beyond-parity damage was returned as a successful complete body".into());
        }
        let scan_failure = self.auto_healing_health().await?;
        for (path, backup) in backups.iter().take(usize::from(original_data_shards)) {
            fs::copy(backup, path)?;
        }
        self.assert_download_sha256(&object_key, &payload_sha)
            .await?;
        self.explicit_repair_and_wait("availability").await?;
        for (path, _) in &backups {
            self.wait_for_path(path, true, Duration::from_secs(30))
                .await?;
        }
        let recovered_from_fault = self
            .wait_for_write_safe("explicit recovery after restoring k shards")
            .await?;
        self.assert_download_sha256(&object_key, &payload_sha)
            .await?;

        let old_layout_before = self.object_version_dirs(&version_id)?.len();
        self.stop_server()?;
        // Enrolled geometry is immutable in v4. Even a valid same-width
        // replacement geometry must be rejected, not silently recode objects.
        self.data_shards = original_width as u16;
        self.parity_shards = 0;
        self.write_config()?;
        if self.start_server("auto-healing-ec-change").await.is_ok() {
            return Err("engine admitted an edited enrolled pool geometry".into());
        }
        self.stop_server()?;
        self.data_shards = original_data_shards;
        self.parity_shards = original_parity_shards;
        self.write_config()?;
        self.start_server("auto-healing-original-geometry-restored")
            .await?;
        self.auth().await?;
        let ec_change_startup = self.wait_for_write_safe("EC change startup").await?;
        self.assert_download_sha256(&object_key, &payload_sha)
            .await?;
        let new_key = format!("{}auto-healing/new-ec-object.bin", self.args.prefix);
        let new_payload = deterministic_payload(256 * 1024, b"n1-new-ec-object");
        let new_sha = sha256_bytes(&new_payload);
        let new_version = self
            .upload_bytes(&new_key, new_payload, "application/octet-stream")
            .await?;
        self.assert_download_sha256(&new_key, &new_sha).await?;
        self.explicit_repair_and_wait("availability").await?;
        let old_layout_after = self.object_version_dirs(&version_id)?.len();
        let new_layout = self.object_version_dirs(&new_version)?.len();
        if old_layout_before != original_width
            || old_layout_after != original_width
            || new_layout != original_width
        {
            return Err(format!(
                "per-object EC topology changed unexpectedly: old_before={old_layout_before} old_after={old_layout_after} new={new_layout}"
            )
            .into());
        }
        let final_health = self.auto_healing_health().await?;

        self.summary = json!({
            "status": "PASS",
            "proof": "n1_auto_healing",
            "fragment_hash": self.fragment,
            "object_key": object_key,
            "version_id": version_id,
            "startup": startup,
            "recover_on_read": {
                "removed_and_restored": removed_on_read
            },
            "explicit_repair": {
                "removed_and_restored": removed_by_scan,
                "status": scheduled_repair
            },
            "read_integrity": read_integrity,
            "concurrent_read_integrity": concurrent_read_integrity,
            "explicit_checksum_gate": explicit_checksum_gate,
            "missing_root": {
                "root": root,
                "health": missing_health,
                "mutation": rejected_mutation,
                "restored": restored_health
            },
            "identity_mismatch": {
                "health": replacement_health,
                "restored": identity_restored_health
            },
            "unrecoverable_fail_safe": {
                "health": scan_failure,
                "recovered": recovered_from_fault
            },
            "ec_change": {
                "enrolled_geometry_edit_rejected": true,
                "startup": ec_change_startup,
                "old_object_recorded_roots_before": old_layout_before,
                "old_object_recorded_roots_after": old_layout_after,
                "new_object_recorded_roots": new_layout,
                "new_object_key": new_key,
                "new_version_id": new_version
            },
            "final_health": final_health
        });
        self.write_summary()?;
        self.stop_server()?;
        if self.args.cleanup_roots {
            self.cleanup_roots()?;
        }
        Ok(())
    }
}
