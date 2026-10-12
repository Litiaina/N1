impl Proof {
    fn interrupt_backup_restore(
        &self,
        repo: &Path,
        key: &Path,
        snapshot: &str,
        config: &Path,
    ) -> ProofResult<usize> {
        // Kill the actual restore process, not a delegating shell parent.
        let log = fs::File::create(self.args.out_dir.join("interrupted-restore.log"))?;
        let mut child = Command::new(&self.args.binary)
            .args(["backup", "restore", "--snapshot", snapshot, "--key-file"])
            .arg(key)
            .arg("--repository")
            .arg(repo)
            .arg("--target-config")
            .arg(config)
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()?;
        let deadline = Instant::now() + Duration::from_secs(120);
        let mut progressed = false;
        while Instant::now() < deadline {
            if child.try_wait()?.is_some() {
                return Err("restore completed before its interruption proof".into());
            }
            if self
                .index_roots
                .iter()
                .all(|r| r.join(".n1-restore-pending").is_file())
                && self.index_roots.iter().any(|r| self.proof_piece(r).is_ok())
            {
                progressed = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        child.kill()?;
        child.wait()?;
        if !progressed {
            return Err("restore never materialized a piece before interruption".into());
        }
        let mut files = 0;
        for root in &self.index_roots {
            if !root.join(".n1-restore-pending").is_file() {
                return Err("interrupted restore lost an activation fence".into());
            }
            let mut directories = vec![root.clone()];
            while let Some(directory) = directories.pop() {
                for entry in fs::read_dir(directory)? {
                    let entry = entry?;
                    if entry.file_type()?.is_dir() {
                        directories.push(entry.path());
                    } else if entry.file_type()?.is_file() {
                        files += 1;
                    }
                }
            }
        }
        Ok(files)
    }

    fn backup_cli(
        &self,
        repo: &Path,
        key: &Path,
        operation: &str,
        args: &[&str],
        success: bool,
    ) -> ProofResult<String> {
        let binary = self
            .args
            .binary
            .parent()
            .ok_or("missing binary parent")?
            .join("litiaina-n1-shell");
        let mut command = Command::new(binary);
        command
            .arg("backup")
            .arg(operation)
            .arg("--key-file")
            .arg(key);
        if operation != "keygen" {
            command.arg("--repository").arg(repo);
        }
        let output = command.args(args).output()?;
        if output.status.success() != success {
            return Err(format!(
                "backup {operation}: status={} stdout={} stderr={}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        Ok(String::from_utf8(output.stdout)?)
    }

    async fn run_backup_proof(&mut self) -> ProofResult<()> {
        self.validate()?;
        self.prepare_dirs()?;
        self.write_config()?;
        let portable_spare = if self.index_roots.len() == 1 {
            self.args
                .spare_root
                .as_ref()
                .map(fs::canonicalize)
                .transpose()?
        } else {
            None
        };
        if let Some(spare) = &portable_spare {
            use std::os::unix::fs::MetadataExt;
            if spare.metadata()?.dev() == self.index_roots[0].metadata()?.dev() {
                return Err("single-member portable restore requires a different device".into());
            }
        }
        let evidence = self.args.out_dir.clone();
        let config = evidence.join("n1_config.ini");
        fs::write(
            &config,
            fs::read_to_string(&config)?.replace("file_ttl_seconds=30", "file_ttl_seconds=86400"),
        )?;
        let key = evidence.join("backup.key");
        let repo = self.disk_roots()?[0].join(format!("n1-backup-repository-{}", self.stamp));
        let replica = portable_spare
            .clone()
            .unwrap_or(
                self.disk_roots()?
                    .last()
                    .ok_or("missing target disk")?
                    .clone(),
            )
            .join(format!("n1-backup-replica-{}", self.stamp));
        self.backup_cli(&repo, &key, "keygen", &[], true)?;
        self.backup_cli(&repo, &key, "init", &[], true)?;
        self.backup_cli(&replica, &key, "init", &[], true)?;
        self.start_server("backup-source").await?;
        self.create_fragment().await?;
        self.auth().await?;
        let mut expected = BTreeSet::new();
        let lineage = self.rename_version_lineage_proof(&mut expected).await?;
        let storm = self
            .concurrent_object_storm(&mut expected, "backup-small", 128)
            .await?;
        let mut baseline_hashes = Vec::new();
        for path in &expected {
            baseline_hashes.push((path.clone(), self.download_sha256(path).await?));
        }
        let other_fragment = self
            .create_named_fragment(
                &format!("backup-other-{}", self.stamp),
                "backup-other-secret",
            )
            .await?;
        let other_token = self
            .auth_fragment(&other_fragment, "backup-other-secret")
            .await?;
        let other_payload = b"independent fragment identity";
        self.upload_bytes_with_token(
            &other_token,
            "other.bin",
            other_payload.to_vec(),
            "application/octet-stream",
        )
        .await?;
        // Empty physical organizers survive backup but are not logical objects.
        for root in &self.index_roots {
            fs::create_dir_all(
                physical_fragment_root(root, &self.stable_fragment_id)
                    .join(".namespace/backup-empty"),
            )?;
        }
        let trash_key = "backup-trash/file.bin";
        let trash_payload = deterministic_payload(65536, b"backup-trash");
        let trash_hash = sha256_bytes(&trash_payload);
        self.upload_bytes(trash_key, trash_payload, "application/octet-stream")
            .await?;
        self.posix_post("/noa/v1/posix/trash", json!({"path":"backup-trash"}))
            .await?;
        let durable_key = "backup-durable.bin";
        let total = self.args.source_file.metadata()?.len();
        let part_size = self.args.max_part_size.min(total.div_ceil(2)).max(1);
        let count = total.div_ceil(part_size);
        let params = json!({"content_type":"application/octet-stream","total_size_bytes":total,"expected_parts":count,"part_size_bytes":part_size});
        let response = self
            .client
            .post(format!(
                "{}/noa/v1/upload/multipart/durable/init/{durable_key}",
                self.base_url
            ))
            .bearer_auth(&self.token)
            .json(&params)
            .send()
            .await?;
        if response.status() != StatusCode::CREATED {
            return Err(format!("durable init: {}", response.text().await?).into());
        }
        let init: MultipartInit = response.json().await?;
        self.upload_durable_part_range(&init.version_id, 0, 1, part_size)
            .await?;
        self.backup_cli(
            &repo,
            &key,
            "create",
            &["--config", config.to_str().ok_or("config path")?],
            false,
        )?;
        self.stop_server()?;
        let capture_start = Instant::now();
        let first: Value = serde_json::from_str(&self.backup_cli(
            &repo,
            &key,
            "create",
            &[
                "--config",
                config.to_str().ok_or("config path")?,
                "--workers",
                "4",
            ],
            true,
        )?)?;
        let capture_seconds = capture_start.elapsed().as_secs_f64();
        let first_id = first["snapshot"]
            .as_str()
            .ok_or("snapshot identity missing")?;
        self.backup_cli(&repo, &key, "verify", &["--snapshot", first_id], true)?;
        let replication_start = Instant::now();
        self.backup_cli(
            &repo,
            &key,
            "replicate",
            &[
                "--snapshot",
                first_id,
                "--target-repository",
                replica.to_str().ok_or("repo path")?,
                "--target-key-file",
                key.to_str().ok_or("key path")?,
            ],
            true,
        )?;
        let replication_seconds = replication_start.elapsed().as_secs_f64();
        self.start_server("backup-incremental-source").await?;
        self.auth().await?;
        let new_payload = deterministic_payload(1024 * 1024, b"incremental-change");
        let new_hash = sha256_bytes(&new_payload);
        let new_version = self
            .upload_bytes("backup-new.bin", new_payload, "application/octet-stream")
            .await?;
        expected.insert("backup-new.bin".to_owned());
        self.stop_server()?;
        let incremental_start = Instant::now();
        let second: Value = serde_json::from_str(&self.backup_cli(
            &repo,
            &key,
            "create",
            &["--config", config.to_str().ok_or("config path")?],
            true,
        )?)?;
        let incremental_seconds = incremental_start.elapsed().as_secs_f64();
        let second_id = second["snapshot"]
            .as_str()
            .ok_or("snapshot identity missing")?;
        self.backup_cli(
            &repo,
            &key,
            "replicate",
            &[
                "--snapshot",
                second_id,
                "--target-repository",
                replica.to_str().ok_or("repo path")?,
                "--target-key-file",
                key.to_str().ok_or("key path")?,
            ],
            true,
        )?;
        self.backup_cli(
            &repo,
            &key,
            "replicate",
            &[
                "--snapshot",
                second_id,
                "--target-repository",
                replica.to_str().ok_or("repo path")?,
                "--target-key-file",
                key.to_str().ok_or("key path")?,
            ],
            true,
        )?;
        // Make the original storage paths unavailable. Successful readback must
        // come from remapped restored shards, never an accidental old absolute path.
        let old_index = self.index_roots.clone();
        let old_blob = self.blob_roots.clone();
        let all: BTreeSet<_> = old_index.iter().chain(&old_blob).cloned().collect();
        for path in all {
            fs::rename(
                &path,
                path.with_file_name(format!(
                    "{}-source-offline",
                    path.file_name().ok_or("root name")?.to_string_lossy()
                )),
            )?;
            fs::create_dir(&path)?;
        }
        self.index_roots = old_index
            .iter()
            .rev()
            .map(|p| {
                p.with_file_name(format!(
                    "{}-restored",
                    p.file_name().unwrap().to_string_lossy()
                ))
            })
            .collect();
        self.blob_roots = old_blob
            .iter()
            .rev()
            .map(|p| {
                p.with_file_name(format!(
                    "{}-restored",
                    p.file_name().unwrap().to_string_lossy()
                ))
            })
            .collect();
        if let Some(spare) = &portable_spare {
            self.index_roots = vec![spare.join(format!("n1-restored-store-{}", self.stamp))];
            self.blob_roots = self.index_roots.clone();
        }
        for root in self.index_roots.iter().chain(&self.blob_roots) {
            fs::create_dir_all(root)?;
        }
        self.args.out_dir = evidence.join("restored-control");
        fs::create_dir_all(self.args.out_dir.join("fs"))?;
        fs::create_dir_all(self.args.out_dir.join("refresh"))?;
        self.write_config()?;
        let restore_config = self.args.out_dir.join("n1_config.ini");
        fs::write(
            &restore_config,
            fs::read_to_string(&restore_config)?
                .replace("file_ttl_seconds=30", "file_ttl_seconds=86400"),
        )?;
        let restore_start = Instant::now();
        let restore_interrupted_files =
            self.interrupt_backup_restore(&replica, &key, second_id, &restore_config)?;
        self.backup_cli(
            &replica,
            &key,
            "restore",
            &[
                "--snapshot",
                second_id,
                "--target-config",
                restore_config.to_str().ok_or("config path")?,
            ],
            false,
        )?;
        if self.start_server("backup-incomplete-restore").await.is_ok() {
            self.stop_server()?;
            return Err("engine admitted an interrupted restore".into());
        }
        self.stop_server()?;
        self.backup_cli(
            &replica,
            &key,
            "restore",
            &[
                "--snapshot",
                second_id,
                "--target-config",
                restore_config.to_str().ok_or("config path")?,
                "--resume",
            ],
            true,
        )?;
        let restore_seconds = restore_start.elapsed().as_secs_f64();
        self.start_server("backup-restored").await?;
        self.auth().await?;
        self.assert_expected_visible(&expected).await?;
        for (path, hash) in &baseline_hashes {
            self.assert_download_sha256(path, hash).await?;
        }
        let restored_other_token = self
            .auth_fragment(&other_fragment, "backup-other-secret")
            .await?;
        self.assert_download_sha256_with_token(
            &restored_other_token,
            "other.bin",
            &sha256_bytes(other_payload),
        )
        .await?;
        self.assert_download_sha256("backup-new.bin", &new_hash)
            .await?;
        if !self
            .list_version_ids("backup-new.bin")
            .await?
            .contains(&new_version)
        {
            return Err("restored version identity changed".into());
        }
        let lineage_key = lineage["new_key"].as_str().ok_or("lineage key missing")?;
        let versions = self.list_version_ids(lineage_key).await?;
        for field in ["first_version", "second_version"] {
            if !versions
                .iter()
                .any(|v| Some(v.as_str()) == lineage[field].as_str())
            {
                return Err("restored version history incomplete".into());
            }
        }
        self.assert_download_sha256(
            lineage_key,
            &sha256_bytes(&deterministic_payload(384 * 1024, b"rename-lineage-v2")),
        )
        .await?;
        for root in &self.index_roots {
            if !physical_fragment_root(root, &self.stable_fragment_id)
                .join(".namespace/backup-empty")
                .is_dir()
            {
                return Err("backup lost an empty physical organizer".into());
            }
        }
        self.posix_post("/noa/v1/posix/recover", json!({"path":"backup-trash"}))
            .await?;
        self.assert_download_sha256(trash_key, &trash_hash).await?;
        self.upload_durable_part_range(&init.version_id, 1, count, part_size)
            .await?;
        let response = self
            .client
            .post(format!(
                "{}/noa/v1/upload/multipart/durable/{}/{}/finalize?object_key={durable_key}",
                self.base_url, init.version_id, init.session_id
            ))
            .bearer_auth(&self.token)
            .json(&params)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "restored durable finalize failed: {}",
                response.text().await?
            )
            .into());
        }
        self.assert_download_sha256(
            durable_key,
            &self.sha256_file(&self.args.source_file).await?,
        )
        .await?;
        self.range_probe(durable_key).await?;
        // New mutations must work after restore, not only static reads.
        let post = self
            .upload_bytes(
                "post-restore.bin",
                b"new write after restore".to_vec(),
                "application/octet-stream",
            )
            .await?;
        self.assert_download_sha256(
            "post-restore.bin",
            &sha256_bytes(b"new write after restore"),
        )
        .await?;
        self.posix_post("/noa/v1/posix/trash", json!({"path":"post-restore.bin"}))
            .await?;
        self.expect_download_status("post-restore.bin", StatusCode::NOT_FOUND)
            .await?;
        self.stop_server()?;
        self.backup_cli(&replica, &key, "verify", &["--snapshot", first_id], true)?;
        self.backup_cli(&repo, &key, "forget", &["--snapshot", first_id], true)?;
        self.backup_cli(&repo, &key, "gc", &["--discard-incomplete"], true)?;
        self.backup_cli(&repo, &key, "verify", &["--snapshot", second_id], true)?;
        self.args.out_dir = evidence;
        self.summary = json!({"status":"PASS","proof":"encrypted_offline_backup_and_snapshot_replication","ec":[self.data_shards,self.parity_shards],
            "first_snapshot":first,"second_snapshot":second,"capture_seconds":capture_seconds,"incremental_capture_seconds":incremental_seconds,
            "replication_seconds":replication_seconds,"restore_seconds":restore_seconds,"restore_interrupted_files":restore_interrupted_files,
            "physical_power_cut":false,"lineage":lineage,"small_objects":storm,
            "single_member_cross_device_restore":portable_spare.is_some(),
            "checks":["online_capture_rejected","encrypted_capture_verified","incremental_snapshot","replication_idempotent","original_roots_offline",
                "portable_restore","sigkill_after_restore_piece_publication","interrupted_restore_startup_rejected","explicit_restore_resume",
                "paginated_listing","version_identity_and_history","download_sha256","empty_physical_organizer","trash_recovery","durable_session_resume",
                "range_download","new_write_and_delete","retained_older_replica","retention_gc_preserves_live_blocks","all_small_object_hashes","independent_fragment_credentials"],
            "post_restore_version":post,"repository":repo,"replica":replica,"key_file":key,
            "test_scope":"isolated directories; no production service restart or disk formatting"});
        self.write_summary()?;
        Ok(())
    }
}
