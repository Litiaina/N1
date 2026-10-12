// Real-device offline maintenance proof. All destructive work is confined to
// prepare_dirs() roots; a bind mount models same-path replacement, not power loss.
struct PoolProofMount(PathBuf);
impl PoolProofMount {
    fn bind(source: &Path, target: &Path) -> ProofResult<Self> {
        let status = Command::new("mount")
            .arg("--bind")
            .arg(source)
            .arg(target)
            .status()?;
        if !status.success() {
            return Err("test bind mount failed".into());
        }
        Ok(Self(target.to_owned()))
    }
}
impl Drop for PoolProofMount {
    fn drop(&mut self) {
        let _ = Command::new("umount").arg("-l").arg(&self.0).status();
    }
}

impl Proof {
    async fn assert_expected_listing(&self, expected: &BTreeSet<String>) -> ProofResult<()> {
        let mut prefixes = vec![String::new()];
        let mut actual = BTreeSet::new();
        while let Some(prefix) = prefixes.pop() {
            let (entries, _) = self.flat_list_all(&prefix, 17).await?;
            for key in entries {
                if key.ends_with('/') {
                    prefixes.push(key);
                } else if !actual.insert(key) {
                    return Err("listing repeated an object".into());
                }
            }
        }
        if actual != *expected {
            return Err(format!(
                "listing mismatch: missing={:?} extra={:?}",
                expected.difference(&actual).collect::<Vec<_>>(),
                actual.difference(expected).collect::<Vec<_>>()
            )
            .into());
        }
        Ok(())
    }
    fn pool_command(&self, operation: &str, extra: &[&str]) -> Command {
        let mut command =
            Command::new(self.args.binary.parent().unwrap().join("litiaina-n1-shell"));
        command
            .arg("pool")
            .arg(operation)
            .arg("--config")
            .arg(self.args.out_dir.join("n1_config.ini"));
        if operation != "verify" {
            command
                .args(["--pool", "A", "--member"])
                .arg(self.args.pool_member.to_string());
        }
        command.args(extra);
        command
    }

    fn pool_cli(
        &self,
        operation: &str,
        extra: &[&str],
        success: bool,
        measures: &mut Vec<Value>,
    ) -> ProofResult<()> {
        let start = Instant::now();
        let output = self.pool_command(operation, extra).output()?;
        if output.status.success() != success {
            return Err(format!(
                "pool {operation}: {} {} {}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        measures.push(json!({"operation":operation,"expected_success":success,"elapsed_ms":start.elapsed().as_secs_f64()*1000.0,
            "report":serde_json::from_slice::<Value>(&output.stdout).ok()}));
        Ok(())
    }

    fn interrupted_pool_cli(
        &self,
        operation: &str,
        extra: &[&str],
        fence: &Path,
    ) -> ProofResult<usize> {
        // Launch the engine directly so SIGKILL cannot leave a delegated child
        // running after killing only its shell parent.
        let log = fs::File::create(
            self.args
                .out_dir
                .join(format!("interrupted-{operation}.log")),
        )?;
        let mut child = Command::new(&self.args.binary)
            .arg("pool")
            .arg(operation)
            .arg("--config")
            .arg(self.args.out_dir.join("n1_config.ini"))
            .args(["--pool", "A", "--member"])
            .arg(self.args.pool_member.to_string())
            .args(extra)
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()?;
        let deadline = Instant::now() + Duration::from_secs(10);
        while (!fence.exists() || self.proof_piece(fence.parent().unwrap()).is_err())
            && Instant::now() < deadline
        {
            if child.try_wait()?.is_some() {
                return Err("maintenance finished before interruption fence".into());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        if self.proof_piece(fence.parent().unwrap()).is_err() {
            child.kill()?;
            child.wait()?;
            return Err("maintenance never materialized a piece before interruption".into());
        }
        if child.try_wait()?.is_some() {
            return Err("maintenance too short for interruption proof".into());
        }
        child.kill()?;
        child.wait()?;
        if !fence.is_file() {
            return Err("interrupted maintenance lost its activation fence".into());
        }
        let mut files = 0;
        let mut directories = vec![fence.parent().unwrap().to_owned()];
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
        Ok(files)
    }

    fn proof_piece(&self, root: &Path) -> ProofResult<PathBuf> {
        let mut dirs = vec![root.to_owned()];
        while let Some(dir) = dirs.pop() {
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    dirs.push(entry.path());
                } else if entry.file_type()?.is_file()
                    && entry
                        .path()
                        .extension()
                        .is_some_and(|e| e == "shard" || e == "parity")
                    && entry.metadata()?.len() > 0
                {
                    return Ok(entry.path());
                }
            }
        }
        Err("proof has no piece to corrupt".into())
    }

    async fn run_pool_maintenance_proof(&mut self) -> ProofResult<()> {
        use std::io::{Read, Seek, Write};
        use std::os::unix::fs::MetadataExt;
        self.validate()?;
        let spare = self
            .args
            .spare_root
            .clone()
            .ok_or("--spare-root is required")?;
        if !spare.is_dir() {
            return Err("spare device directory must exist".into());
        }
        self.prepare_dirs()?;
        if self.args.pool_member >= self.index_roots.len() {
            return Err("test member outside pool".into());
        }
        for root in &self.index_roots {
            if root.metadata()?.dev() == spare.metadata()?.dev() {
                return Err("spare must be a different filesystem from every pool member".into());
            }
        }
        self.write_config()?;
        let config = self.args.out_dir.join("n1_config.ini");
        fs::write(
            &config,
            fs::read_to_string(&config)?.replace("file_ttl_seconds=30", "file_ttl_seconds=86400"),
        )?;
        self.start_server("pool-source").await?;
        self.create_fragment().await?;
        self.auth().await?;
        let mut expected = BTreeSet::new();
        self.upload_bytes("empty.bin", Vec::new(), "application/octet-stream")
            .await?;
        expected.insert("empty.bin".into());
        let lineage = self.rename_version_lineage_proof(&mut expected).await?;
        self.concurrent_object_storm(&mut expected, "pool-maintenance", 128)
            .await?;
        let mut hashes = Vec::new();
        for key in &expected {
            hashes.push((key.clone(), self.download_sha256(key).await?));
        }
        let trash = deterministic_payload(65536, b"pool-trash");
        let trash_hash = sha256_bytes(&trash);
        self.upload_bytes("pool-trash/file.bin", trash, "application/octet-stream")
            .await?;
        self.posix_post("/noa/v1/posix/trash", json!({"path":"pool-trash"}))
            .await?;
        let total = self.args.source_file.metadata()?.len();
        let part_size = self.args.max_part_size.min(total.div_ceil(2)).max(1);
        let count = total.div_ceil(part_size);
        let response = self.client.post(format!("{}/noa/v1/upload/multipart/durable/init/pool-durable.bin",self.base_url))
            .bearer_auth(&self.token).json(&json!({"content_type":"application/octet-stream","total_size_bytes":total,"expected_parts":count,"part_size_bytes":part_size})).send().await?;
        if response.status() != StatusCode::CREATED {
            return Err(response.text().await?.into());
        }
        let init: MultipartInit = response.json().await?;
        self.upload_durable_part_range(&init.version_id, 0, 1, part_size)
            .await?;
        let candidate = spare.join(format!("n1-pool-clone-{}", self.stamp));
        fs::create_dir(&candidate)?;
        let copy_args = ["--copy-root", candidate.to_str().ok_or("candidate path")?];
        let mut measures = Vec::new();
        self.pool_cli("verify", &[], false, &mut measures)?;
        self.pool_cli("clone", &copy_args, false, &mut measures)?;
        self.stop_server()?;
        self.pool_cli("verify", &[], true, &mut measures)?;
        let replica_faults_tested = self.index_roots.len() > 1;
        if replica_faults_tested {
            let session =
                physical_fragment_root(self.index_roots.last().unwrap(), &self.stable_fragment_id)
                    .join(".uploads")
                    .join(&init.version_id);
            let manifest = session.join("manifest.ndx");
            let original = fs::read(&manifest)?;
            let mut divergent = original.clone();
            *divergent.last_mut().ok_or("empty durable manifest")? ^= 1;
            fs::write(&manifest, divergent)?;
            self.pool_cli("verify", &[], false, &mut measures)?;
            fs::write(&manifest, original)?;
            let receipt = fs::read_dir(&session)?
                .collect::<std::io::Result<Vec<_>>>()?
                .into_iter()
                .find(|e| e.path().extension().is_some_and(|x| x == "receipt"))
                .ok_or("missing acknowledged durable receipt")?
                .path();
            let original = fs::read(&receipt)?;
            fs::remove_file(&receipt)?;
            self.pool_cli("verify", &[], false, &mut measures)?;
            fs::write(&receipt, original)?;
            self.pool_cli("verify", &[], true, &mut measures)?;
        }
        let before = self.flat_immutable_digest(&self.index_roots)?;
        self.pool_cli("prepare-copy", &copy_args, true, &mut measures)?;
        self.pool_cli("verify-copy", &copy_args, false, &mut measures)?;
        let clone_interrupted_files = self.interrupted_pool_cli(
            "clone",
            &[copy_args[0], copy_args[1], "--resume"],
            &candidate.join(".n1-restore-pending"),
        )?;
        self.pool_cli(
            "clone",
            &[copy_args[0], copy_args[1], "--resume"],
            true,
            &mut measures,
        )?;
        if before != self.flat_immutable_digest(&self.index_roots)? {
            return Err("healthy clone altered source NDX/payload".into());
        }
        let piece = self.proof_piece(&candidate)?;
        let mut file = fs::OpenOptions::new().read(true).write(true).open(&piece)?;
        let mut byte = [0u8];
        file.read_exact(&mut byte)?;
        file.seek(std::io::SeekFrom::Start(0))?;
        file.write_all(&[byte[0] ^ 0x55])?;
        file.sync_all()?;
        drop(file);
        self.pool_cli("verify-copy", &copy_args, false, &mut measures)?;
        if !candidate.join(".n1-restore-pending").is_file() {
            return Err("bad copy was not fenced".into());
        }
        {
            let mount = PoolProofMount::bind(&candidate, &self.index_roots[self.args.pool_member])?;
            if self.start_server("pool-incomplete-copy").await.is_ok() {
                self.stop_server()?;
                return Err("engine admitted a fenced corrupt copy".into());
            }
            self.stop_server()?;
            drop(mount);
        }
        fs::copy(
            self.index_roots[self.args.pool_member].join(piece.strip_prefix(&candidate)?),
            &piece,
        )?;
        self.pool_cli("verify-copy", &copy_args, true, &mut measures)?;
        {
            let mount = PoolProofMount::bind(&candidate, &self.index_roots[self.args.pool_member])?;
            self.start_server("pool-cloned-device").await?;
            self.auth().await?;
            for (key, hash) in &hashes {
                self.assert_download_sha256(key, hash).await?;
            }
            self.assert_expected_listing(&expected).await?;
            self.stop_server()?;
            drop(mount);
        }
        let replacement = spare.join(format!("n1-pool-replacement-{}", self.stamp));
        fs::create_dir(&replacement)?;
        let mount = PoolProofMount::bind(&replacement, &self.index_roots[self.args.pool_member])?;
        let mut replace_interrupted_files = None;
        if self.parity_shards == 0 {
            self.pool_cli("replace", &["--authorize"], false, &mut measures)?;
            drop(mount);
        } else {
            replace_interrupted_files = Some(self.interrupted_pool_cli(
                "replace",
                &["--authorize"],
                &replacement.join(".n1-restore-pending"),
            )?);
            if replacement.join(".n1-root-id").exists() {
                return Err("partial replacement was trusted before verification".into());
            }
            self.pool_cli("verify", &[], false, &mut measures)?;
            if self
                .start_server("pool-incomplete-replacement")
                .await
                .is_ok()
            {
                self.stop_server()?;
                return Err("engine admitted an unfinished replacement".into());
            }
            self.stop_server()?;
            self.pool_cli("replace", &["--authorize", "--resume"], true, &mut measures)?;
            if before != self.flat_immutable_digest(&self.index_roots)? {
                return Err("reconstruction changed committed NDX/payload bytes".into());
            }
            self.pool_cli("verify", &[], true, &mut measures)?;
            self.start_server("pool-replaced-device").await?;
            self.auth().await?;
            for (key, hash) in &hashes {
                self.assert_download_sha256(key, hash).await?;
            }
            self.assert_expected_listing(&expected).await?;
            self.stop_server()?;
            drop(mount);
        }
        // Continue against the validated copied device; original healthy data
        // remains untouched below the test mount for operator rollback.
        let final_device = if self.parity_shards == 0 {
            &candidate
        } else {
            &replacement
        };
        let mount = PoolProofMount::bind(final_device, &self.index_roots[self.args.pool_member])?;
        self.start_server("pool-post-cutover").await?;
        self.auth().await?;
        self.upload_durable_part_range(&init.version_id, 1, count, part_size)
            .await?;
        let response = self.client.post(format!("{}/noa/v1/upload/multipart/durable/{}/{}/finalize?object_key=pool-durable.bin",self.base_url,init.version_id,init.session_id))
            .bearer_auth(&self.token).json(&json!({"content_type":"application/octet-stream","total_size_bytes":total,"expected_parts":count,"part_size_bytes":part_size})).send().await?;
        if !response.status().is_success() {
            return Err(format!(
                "durable continuation after device change: {}",
                response.text().await?
            )
            .into());
        }
        self.assert_download_sha256(
            "pool-durable.bin",
            &self.sha256_file(&self.args.source_file).await?,
        )
        .await?;
        expected.insert("pool-durable.bin".into());
        self.posix_post("/noa/v1/posix/recover", json!({"path":"pool-trash"}))
            .await?;
        self.assert_download_sha256("pool-trash/file.bin", &trash_hash)
            .await?;
        expected.insert("pool-trash/file.bin".into());
        let payload = b"post device cutover mutation";
        self.upload_bytes(
            "post-cutover.bin",
            payload.to_vec(),
            "application/octet-stream",
        )
        .await?;
        self.posix_post(
            "/noa/v1/posix/rename",
            json!({"source_path":"post-cutover.bin","destination_path":"post-renamed.bin"}),
        )
        .await?;
        self.assert_download_sha256("post-renamed.bin", &sha256_bytes(payload))
            .await?;
        expected.insert("post-renamed.bin".into());
        self.assert_expected_listing(&expected).await?;
        self.stop_server()?;
        self.pool_cli("verify", &[], true, &mut measures)?;
        drop(mount);
        self.summary = json!({"status":"PASS","proof":"pool_maintenance","ec":[self.data_shards,self.parity_shards],"member":self.args.pool_member,"objects":expected.len(),"lineage":lineage,
            "checks":["active_engine_exclusion","partial_copy_rejection","same_size_corruption_rejection","sigkill_clone_resume","byte_exact_source_immutability","cross_device_same_path_clone","listing_readback","blank_member_refusal","verified_identity_activation","trash_recovery","durable_receipt_continuation","post_cutover_upload_and_rename"],
            "replacement_tested":self.parity_shards>0,"clone_interrupted_files":clone_interrupted_files,"replace_interrupted_files":replace_interrupted_files,"measurements":measures,"physical_power_cut":false,"replica_faults_tested":replica_faults_tested});
        self.write_summary()?;
        Ok(())
    }
}
