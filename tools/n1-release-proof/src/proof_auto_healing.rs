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
        self.posix_post("/noa/v1/posix/mkdir", json!({"path": &base}))
            .await?;
        self.posix_post("/noa/v1/posix/mkdir", json!({"path": &nested}))
            .await?;

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
        let removed_after_page_boundary =
            self.remove_one_shard_matching(last_version, |path| {
                path.extension().and_then(|ext| ext.to_str()) == Some("parity")
            })?;

        self.stop_server()?;
        self.start_server("trash-maintenance-after-restart").await?;
        self.auth().await?;
        self.wait_for_path(&removed_direct, true, Duration::from_secs(30))
            .await?;
        self.wait_for_path(
            &removed_after_page_boundary,
            true,
            Duration::from_secs(30),
        )
        .await?;
        let scan_health = self
            .wait_for_auto_healing_state(
                "successful restarted trash maintenance scan",
                Duration::from_secs(30),
                |snapshot| {
                    snapshot
                        .pointer("/maintenance/auto_healing/last_availability_scan_at")
                        .is_some_and(Value::is_number)
                        && snapshot
                            .pointer("/maintenance/auto_healing/write_safe")
                            .and_then(Value::as_bool)
                            == Some(true)
                        && snapshot
                            .pointer("/maintenance/access_mode")
                            .and_then(Value::as_str)
                            == Some("availability")
                },
            )
            .await?;

        self.posix_post("/noa/v1/posix/recover", json!({"path": &direct_key}))
            .await?;
        self.posix_post("/noa/v1/posix/recover", json!({"path": &base}))
            .await?;
        self.assert_download_sha256(&direct_key, &direct_sha).await?;
        for (key, sha, _) in &directory_objects {
            self.assert_download_sha256(key, sha).await?;
        }
        let final_health = self.wait_for_write_safe("post-recovery write safety").await?;

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

    async fn prove_scheduled_checksum_remains_online(&self) -> ProofResult<Value> {
        let deadline = Instant::now() + Duration::from_secs(45);
        let mut last = json!(null);
        let mut attempt = 0usize;
        while Instant::now() < deadline {
            last = self.auto_healing_health().await?;
            let automatic_checksum_running = last
                .pointer("/maintenance/auto_healing/phase")
                .and_then(Value::as_str)
                == Some("running_checksum_scrub");
            if automatic_checksum_running {
                let access_mode = last
                    .pointer("/maintenance/access_mode")
                    .and_then(Value::as_str)
                    .unwrap_or("missing");
                if access_mode != "availability" {
                    return Err(format!(
                        "scheduled checksum entered {access_mode} instead of preserving availability: {last}"
                    )
                    .into());
                }

                let key = format!(
                    "{}auto-healing/online-checksum-write-{attempt:03}.txt",
                    self.args.prefix
                );
                let (status, body) = self.raw_one_shot_mutation(&key).await?;
                if status != StatusCode::CREATED {
                    return Err(format!(
                        "scheduled checksum blocked a foreground mutation: status={status} body={body} health={last}"
                    )
                    .into());
                }
                let after_write = self.auto_healing_health().await?;
                return Ok(json!({
                    "running_health": last,
                    "write_status": status.as_u16(),
                    "write_object_key": key,
                    "health_after_write": after_write,
                }));
            }
            attempt += 1;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(format!(
            "timed out observing a scheduled checksum while online; last={last}"
        )
        .into())
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

        let startup = self.wait_for_write_safe("startup topology validation").await?;
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
        self.assert_download_sha256(&object_key, &payload_sha).await?;
        self.wait_for_path(&removed_on_read, true, Duration::from_secs(30))
            .await?;
        self.assert_object_status_clean(&object_key, "recover_on_read")
            .await?;

        let removed_by_scan = self.remove_one_shard_matching(&version_id, |path| {
            path.extension().and_then(|ext| ext.to_str()) == Some("parity")
        })?;
        self.wait_for_path(&removed_by_scan, true, Duration::from_secs(30))
            .await?;
        let scheduled_repair = self
            .wait_for_auto_healing_state(
                "scheduled availability repair",
                Duration::from_secs(30),
                |snapshot| {
                    snapshot
                        .pointer("/maintenance/auto_healing/last_availability_scan_at")
                        .is_some_and(Value::is_number)
                },
            )
            .await?;
        self.assert_object_status_clean(&object_key, "scheduled_auto_repair")
            .await?;

        let checksum_key = format!("{}auto-healing/checksum-baseline.bin", self.args.prefix);
        let checksum_payload =
            deterministic_payload(128 * 1024 * 1024, b"n1-online-checksum-proof");
        let checksum_sha = sha256_bytes(&checksum_payload);
        self.upload_complete_multipart_payload(
            &checksum_key,
            &checksum_payload,
            32 * 1024 * 1024,
        )
        .await?;
        let scheduled_checksum_online = self.prove_scheduled_checksum_remains_online().await?;
        self.wait_for_auto_healing_state(
            "completed scheduled online checksum",
            Duration::from_secs(45),
            |snapshot| {
                snapshot
                    .pointer("/maintenance/auto_healing/last_checksum_scrub_at")
                    .is_some_and(Value::is_number)
                    && snapshot
                        .pointer("/maintenance/auto_healing/phase")
                        .and_then(Value::as_str)
                        == Some("idle")
                    && snapshot
                        .pointer("/maintenance/access_mode")
                        .and_then(Value::as_str)
                        == Some("availability")
            },
        )
        .await?;
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
        self.assert_download_sha256(&object_key, &payload_sha).await?;
        let rejected_mutation = self
            .assert_mutation_service_unavailable(&format!(
                "{}auto-healing/rejected.bin",
                self.args.prefix
            ))
            .await?;
        missing.restore()?;
        let restored_health = self.wait_for_write_safe("restored-root recovery").await?;
        self.assert_download_sha256(&object_key, &payload_sha).await?;

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
        let scan_failure = self
            .wait_for_auto_healing_state(
                "unrecoverable scan fail-safe",
                Duration::from_secs(30),
                |snapshot| {
                    snapshot
                        .pointer("/maintenance/auto_healing/phase")
                        .and_then(Value::as_str)
                        == Some("scan_failed")
                        && snapshot
                            .pointer("/maintenance/access_mode")
                            .and_then(Value::as_str)
                            == Some("read_only")
                },
            )
            .await?;
        for (path, backup) in backups.iter().take(usize::from(original_data_shards)) {
            fs::copy(backup, path)?;
        }
        for (path, _) in &backups {
            self.wait_for_path(path, true, Duration::from_secs(30))
                .await?;
        }
        let recovered_from_fault = self
            .wait_for_write_safe("successful automatic retry after repair became possible")
            .await?;
        self.assert_download_sha256(&object_key, &payload_sha).await?;

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
        self.start_server("auto-healing-original-geometry-restored").await?;
        self.auth().await?;
        let ec_change_startup = self.wait_for_write_safe("EC change startup").await?;
        self.assert_download_sha256(&object_key, &payload_sha).await?;
        let new_key = format!("{}auto-healing/new-ec-object.bin", self.args.prefix);
        let new_payload = deterministic_payload(256 * 1024, b"n1-new-ec-object");
        let new_sha = sha256_bytes(&new_payload);
        let new_version = self
            .upload_bytes(&new_key, new_payload, "application/octet-stream")
            .await?;
        self.assert_download_sha256(&new_key, &new_sha).await?;
        self.wait_for_auto_healing_state(
            "post-EC-change autonomous scan",
            Duration::from_secs(30),
            |snapshot| {
                snapshot
                    .pointer("/maintenance/auto_healing/last_availability_scan_at")
                    .is_some_and(Value::is_number)
            },
        )
        .await?;
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
            "recover_on_read": {"removed_and_restored": removed_on_read},
            "scheduled_repair": {"removed_and_restored": removed_by_scan, "health": scheduled_repair},
            "scheduled_checksum_online": scheduled_checksum_online,
            "missing_root": {"root": root, "health": missing_health, "mutation": rejected_mutation, "restored": restored_health},
            "identity_mismatch": {"health": replacement_health, "restored": identity_restored_health},
            "unrecoverable_fail_safe": {"health": scan_failure, "recovered": recovered_from_fault},
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
