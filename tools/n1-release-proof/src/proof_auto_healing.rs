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
    async fn auto_healing_health(&self) -> ProofResult<Value> {
        let response = self
            .client
            .get(format!(
                "{}/n1/maintenance/health?refresh=true",
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
                "{}/n1/upload/one-shot/{}",
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

    async fn run_auto_healing_proof(&mut self) -> ProofResult<()> {
        self.validate()?;
        if self.data_shards != 1 || self.parity_shards != 1 {
            return Err("auto-healing proof currently requires the minimum 1+1 EC topology".into());
        }
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
        if initial_shards.len() != 2 {
            return Err(format!(
                "1+1 object did not create exactly two shards: {initial_shards:?}"
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
            fs::remove_file(path)?;
            backups.push((path.clone(), backup));
        }
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
        fs::copy(&backups[0].1, &backups[0].0)?;
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
        self.data_shards = 1;
        self.parity_shards = 0;
        self.write_config()?;
        self.start_server("auto-healing-ec-change").await?;
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
        if old_layout_before != 2 || old_layout_after != 2 || new_layout != 1 {
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
            "missing_root": {"root": root, "health": missing_health, "mutation": rejected_mutation, "restored": restored_health},
            "identity_mismatch": {"health": replacement_health, "restored": identity_restored_health},
            "unrecoverable_fail_safe": {"health": scan_failure, "recovered": recovered_from_fault},
            "ec_change": {
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
